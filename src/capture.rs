use std::cell::UnsafeCell;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::config::FRAME_SIZE;

/// Pixel layout supplied by a [`FrameSource`].
///
/// The wire protocol currently only carries 640x480 YUYV frames, but capture
/// backends must describe their native output honestly. A screen-capture
/// backend, for example, will normally produce BGRA pixels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code)] // BGRA becomes live with the first screen-capture backend.
pub enum PixelFormat {
    Yuyv422,
    Bgra8888,
}

/// Metadata for the bytes a source wrote into a [`FrameSlot`].
#[derive(Clone, Copy, Debug)]
pub struct FrameInfo {
    /// When this source started waiting for the frame now in the slot.
    pub capture_begins_at: Instant,
    pub width: u32,
    pub height: u32,
    pub format: PixelFormat,
    pub byte_len: usize,
    pub captured_at: Instant,
}

/// Reusable storage for one captured frame.
///
/// `pixels` is allocated once when the pool starts. `info` is `None` while a
/// slot is free, and is set by the capture stage before the slot is published
/// as ready. The SPSC rings added in the next step, rather than this type,
/// establish which stage currently owns a slot.
pub struct FrameSlot {
    pixels: Vec<u8>,
    info: Option<FrameInfo>,
}

impl FrameSlot {
    pub fn new(byte_capacity: usize) -> Self {
        Self {
            pixels: vec![0; byte_capacity],
            info: None,
        }
    }

    pub fn capacity(&self) -> usize {
        self.pixels.len()
    }

    pub fn bytes_mut(&mut self, byte_len: usize) -> Result<&mut [u8], Box<dyn std::error::Error>> {
        let capacity = self.pixels.len();
        self.pixels.get_mut(..byte_len).ok_or_else(|| {
            format!("source needs {byte_len} bytes but slot holds {capacity} bytes").into()
        })
    }

    pub fn bytes(&self, byte_len: usize) -> Result<&[u8], Box<dyn std::error::Error>> {
        self.pixels.get(..byte_len).ok_or_else(|| {
            format!(
                "source wrote {byte_len} bytes into a {} byte slot",
                self.pixels.len()
            )
            .into()
        })
    }

    /// Marks the slot as containing one complete captured frame.
    ///
    /// The capture stage must call this only after it has written all
    /// `info.byte_len` bytes. The future ready-ring publication will make both
    /// the bytes and this metadata visible to the sender together.
    pub fn set_info(&mut self, info: FrameInfo) -> Result<(), Box<dyn std::error::Error>> {
        if info.byte_len > self.capacity() {
            return Err(format!(
                "frame metadata describes {} bytes but slot holds {} bytes",
                info.byte_len,
                self.capacity()
            )
            .into());
        }
        self.info = Some(info);
        Ok(())
    }

    pub fn info(&self) -> Option<FrameInfo> {
        self.info
    }

    /// Clears metadata when the sender has finished with this slot and
    /// returns its ID to the free ring.
    pub fn clear_info(&mut self) {
        self.info = None;
    }
}

/// Identifies one permanent location in a [`FramePool`].
///
/// This is intentionally distinct from a video frame number: a slot is reused
/// for many captured frames over the lifetime of the program.
#[derive(Debug, Eq, PartialEq)]
pub struct SlotId(u8);

impl SlotId {
    fn index(&self) -> usize {
        self.0 as usize
    }
}

/// Fixed, preallocated storage for raw captured frames.
///
/// The pool owns the pixels; SPSC rings carry only [`SlotId`]s.
/// A `Vec` is appropriate because slot IDs are dense, stable indexes from zero
/// to `len - 1`. A hash map would add indirection without representing a
/// problem this pool has.
pub struct FramePool {
    slots: Box<[UnsafeCell<FrameSlot>]>,
}

// A slot is accessed through this shared pool only after a FreeSlots or
// ReadySlots handoff gives one stage exclusive ownership of its SlotId. The
// unsafe accessors below make that precondition explicit at the one boundary
// where shared access becomes a mutable FrameSlot.
unsafe impl Sync for FramePool {}

impl FramePool {
    pub fn new(slot_count: usize, slot_byte_capacity: usize) -> Self {
        assert!(slot_count > 0, "a frame pool needs at least one slot");
        assert!(
            slot_count <= u8::MAX as usize + 1,
            "a u8 SlotId can address at most 256 slots"
        );

        let slots = (0..slot_count)
            .map(|_| UnsafeCell::new(FrameSlot::new(slot_byte_capacity)))
            .collect();

        Self { slots }
    }

    /// The IDs used to seed the free-slot SPSC ring at startup.
    pub fn slot_ids(&self) -> impl Iterator<Item = SlotId> + '_ {
        (0..self.slots.len()).map(|index| SlotId(index as u8))
    }

    /// Gives capture mutable access to a slot it exclusively claimed from
    /// FreeSlots.
    ///
    /// # Safety
    ///
    /// `id` must be owned by capture and must not appear in either ring or be
    /// held by sender. The caller must not let the returned reference escape
    /// the ownership interval.
    pub(crate) unsafe fn capture_slot(&self, id: &SlotId) -> &mut FrameSlot {
        // SAFETY: upheld by the caller's SPSC ownership proof.
        unsafe { &mut *self.slots[id.index()].get() }
    }

    /// Gives sender read-only access to a slot it exclusively claimed from
    /// ReadySlots.
    ///
    /// # Safety
    ///
    /// `id` must be owned by sender and must not appear in either ring or be
    /// held by capture. The caller must not let the returned reference escape
    /// the ownership interval.
    pub(crate) unsafe fn sender_slot(&self, id: &SlotId) -> &FrameSlot {
        // SAFETY: upheld by the caller's SPSC ownership proof.
        unsafe { &*self.slots[id.index()].get() }
    }

    /// Clears a slot immediately before its ID is returned to FreeSlots.
    ///
    /// # Safety
    ///
    /// `id` must be exclusively owned by the stage returning it.
    pub(crate) unsafe fn clear_claimed_slot(&self, id: &SlotId) {
        // SAFETY: upheld by the caller's SPSC ownership proof.
        unsafe { (&mut *self.slots[id.index()].get()).clear_info() };
    }
}

/// A producer of raw image frames.
///
/// A source owns the operating-system capture API. The caller owns the frame
/// storage, so a source does not allocate one `Vec` per frame. The returned
/// metadata says exactly what the source wrote into that storage.
pub trait FrameSource {
    fn next_frame(&mut self, slot: &mut FrameSlot)
    -> Result<FrameInfo, Box<dyn std::error::Error>>;

    /// Waits for and discards one source frame when no pool slot is available.
    /// This prevents the source driver's own queue from becoming a hidden
    /// backlog of old video.
    fn discard_next_frame(&mut self) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct FileCapture {
    reader: BufReader<File>,
    total_frames: u64,
    index: u64,
}

impl FileCapture {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let total_frames = file.metadata()?.len() / FRAME_SIZE as u64;
        if total_frames == 0 {
            return Err("file is smaller than one frame".into());
        }
        Ok(Self {
            reader: BufReader::new(file),
            total_frames,
            index: 0,
        })
    }

    fn begin_next_frame(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.index >= self.total_frames {
            self.reader.rewind()?;
            self.index = 0;
        }
        Ok(())
    }

    fn finish_frame(&mut self) {
        self.index += 1;
        sleep(Duration::from_millis(33));
    }
}

impl FrameSource for FileCapture {
    fn next_frame(
        &mut self,
        slot: &mut FrameSlot,
    ) -> Result<FrameInfo, Box<dyn std::error::Error>> {
        let capture_begins_at = Instant::now();
        self.begin_next_frame()?;
        self.reader.read_exact(slot.bytes_mut(FRAME_SIZE)?)?;
        self.finish_frame();
        Ok(FrameInfo {
            capture_begins_at,
            width: crate::config::WIDTH as u32,
            height: crate::config::HEIGHT as u32,
            format: PixelFormat::Yuyv422,
            byte_len: FRAME_SIZE,
            captured_at: Instant::now(),
        })
    }

    fn discard_next_frame(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.begin_next_frame()?;
        self.reader.seek(SeekFrom::Current(FRAME_SIZE as i64))?;
        self.finish_frame();
        Ok(())
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use linuxvideo::Device;
    use linuxvideo::format::{PixFormat, PixelFormat as V4l2PixelFormat};
    use linuxvideo::stream::ReadStream;

    use std::time::Instant;

    use super::{FrameInfo, FrameSlot, FrameSource, PixelFormat};
    use crate::config::{FRAME_SIZE, HEIGHT, WIDTH};

    pub struct V4l2Capture {
        stream: ReadStream,
    }

    impl V4l2Capture {
        pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let device = Device::open(path)?;
            let capture = device.video_capture(PixFormat::new(
                WIDTH as u32,
                HEIGHT as u32,
                V4l2PixelFormat::YUYV,
            ))?;
            let format = capture.format();
            if format.width() != WIDTH as u32 || format.height() != HEIGHT as u32 {
                return Err(format!("driver gave {}x{}", format.width(), format.height()).into());
            }
            Ok(Self {
                stream: capture.into_stream()?,
            })
        }
    }

    impl FrameSource for V4l2Capture {
        fn next_frame(
            &mut self,
            slot: &mut FrameSlot,
        ) -> Result<FrameInfo, Box<dyn std::error::Error>> {
            let capture_begins_at = Instant::now();
            self.stream.dequeue(|buf| {
                if buf.len() < FRAME_SIZE {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("camera returned {} bytes, need {FRAME_SIZE}", buf.len()),
                    ));
                }
                slot.bytes_mut(FRAME_SIZE)
                    .map_err(|error| std::io::Error::other(error.to_string()))?
                    .copy_from_slice(&buf[..FRAME_SIZE]);
                Ok(())
            })?;
            Ok(FrameInfo {
                capture_begins_at,
                width: WIDTH as u32,
                height: HEIGHT as u32,
                format: PixelFormat::Yuyv422,
                byte_len: FRAME_SIZE,
                captured_at: Instant::now(),
            })
        }

        fn discard_next_frame(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.stream.dequeue(|_| Ok(()))?;
            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::V4l2Capture;

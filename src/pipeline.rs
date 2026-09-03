//! Ownership-preserving capture-to-sender handoff.
//!
//! The only code that crosses from a SlotId to an `UnsafeCell<FrameSlot>` is
//! here. Capture and sender receive safe, role-specific operations instead of
//! arbitrary pool access.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::capture::{FrameInfo, FramePool, FrameSource, SlotId, StreamSpec};
use crate::config::FRAME_POOL_SLOTS;
use crate::spsc::SpscSlotRing;

pub struct Pipeline {
    capture: CapturePort,
    sender: SenderPort,
}

#[derive(Clone)]
pub struct CapturePort {
    pool: Arc<FramePool>,
    free_slots: Arc<SpscSlotRing>,
    ready_slots: Arc<SpscSlotRing>,
    counters: Arc<HandoffCounters>,
    capture_running: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct SenderPort {
    pool: Arc<FramePool>,
    free_slots: Arc<SpscSlotRing>,
    ready_slots: Arc<SpscSlotRing>,
    counters: Arc<HandoffCounters>,
    capture_running: Arc<AtomicBool>,
}

struct HandoffCounters {
    capture_dropped_no_free_slot: AtomicU64,
    sender_dropped_stale_ready: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct HandoffSnapshot {
    pub capture_dropped_no_free_slot: u64,
    pub sender_dropped_stale_ready: u64,
}

/// Result of offering one completed source frame to the capture side of the
/// pool. Dropping at this boundary is intentional: it is always better to
/// discard a newly-arrived frame than to make a live pipeline show an older
/// one later.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapturePublish {
    Published,
    DroppedNoFreeSlot,
}

impl Pipeline {
    pub fn new(stream: StreamSpec) -> Self {
        let pool = Arc::new(FramePool::new(FRAME_POOL_SLOTS, stream.byte_len));
        let free_slots = Arc::new(SpscSlotRing::new());
        let ready_slots = Arc::new(SpscSlotRing::new());

        // Setup occurs before either endpoint is moved to a thread. Main is
        // temporarily the sole FreeSlots producer here.
        for id in pool.slot_ids() {
            free_slots
                .try_push(id)
                .expect("the free ring must hold every pool SlotId at startup");
        }

        let counters = Arc::new(HandoffCounters {
            capture_dropped_no_free_slot: AtomicU64::new(0),
            sender_dropped_stale_ready: AtomicU64::new(0),
        });
        let capture_running = Arc::new(AtomicBool::new(true));

        Self {
            capture: CapturePort {
                pool: Arc::clone(&pool),
                free_slots: Arc::clone(&free_slots),
                ready_slots: Arc::clone(&ready_slots),
                counters: Arc::clone(&counters),
                capture_running: Arc::clone(&capture_running),
            },
            sender: SenderPort {
                pool,
                free_slots,
                ready_slots,
                counters,
                capture_running,
            },
        }
    }

    pub fn into_ports(self) -> (CapturePort, SenderPort) {
        (self.capture, self.sender)
    }
}

impl CapturePort {
    pub fn stop(&self) {
        self.capture_running.store(false, Ordering::Release);
    }

    /// Captures one source frame into a free pool slot, or dequeues and drops a
    /// source frame if sender currently owns every slot.
    pub fn capture_once(
        &self,
        source: &mut impl FrameSource,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(id) = self.free_slots.try_pop() else {
            source.discard_next_frame()?;
            self.counters
                .capture_dropped_no_free_slot
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        };

        let captured = unsafe {
            // SAFETY: popping from FreeSlots gives capture exclusive ownership
            // of `id` until it is either published or returned below.
            let slot = self.pool.capture_slot(&id);
            let info = source.next_frame(slot)?;
            slot.set_info(info)?;
            Ok::<(), Box<dyn std::error::Error>>(())
        };

        // A source failure terminates capture, so this slot is intentionally
        // not returned. Capture must never produce FreeSlots; sender is that
        // ring's sole producer.
        captured?;

        if let Err(id) = self.ready_slots.try_push(id) {
            // This cannot occur in a correct four-slot conservation cycle: a
            // slot just removed from FreeSlots guarantees ReadySlots cannot
            // contain all four IDs. Returning it here would make capture a
            // second FreeSlots producer and violate SPSC, so fail loudly.
            panic!("ReadySlots full after capture claimed {id:?}");
        }
        Ok(())
    }

    /// Copies one externally-owned, possibly strided image into a free slot
    /// and publishes the slot ID to `ReadySlots`.
    ///
    /// PipeWire owns its buffers and reclaims them as soon as its process
    /// callback returns. A screen source therefore cannot hand their pointers
    /// through our pool. This is the one required copy: row-by-row packing
    /// makes every downstream stage see a tight `width * bytes_per_pixel`
    /// layout, regardless of the source stride.
    ///
    /// The method never waits. If sender owns every slot, the just-arrived
    /// source frame is dropped and the source callback can return its buffer
    /// to the OS immediately.
    pub fn publish_strided(
        &self,
        mut info: FrameInfo,
        source: &[u8],
        source_stride: usize,
    ) -> Result<CapturePublish, Box<dyn std::error::Error>> {
        let stream = info.stream_spec()?;
        let row_bytes = (stream.width as usize)
            .checked_mul(stream.format.bytes_per_pixel())
            .ok_or("screen row byte length overflow")?;
        if source_stride < row_bytes {
            return Err(format!(
                "source stride {source_stride} is smaller than one {row_bytes}-byte image row"
            )
            .into());
        }
        let needed = source_stride
            .checked_mul(stream.height.saturating_sub(1) as usize)
            .and_then(|before_last| before_last.checked_add(row_bytes))
            .ok_or("strided source byte length overflow")?;
        if source.len() < needed {
            return Err(format!(
                "source contains {} bytes; {stream:?} with stride {source_stride} needs {needed}",
                source.len()
            )
            .into());
        }

        let Some(id) = self.free_slots.try_pop() else {
            self.counters
                .capture_dropped_no_free_slot
                .fetch_add(1, Ordering::Relaxed);
            return Ok(CapturePublish::DroppedNoFreeSlot);
        };

        unsafe {
            // SAFETY: removing this ID from FreeSlots gives this capture
            // producer exclusive access until ReadySlots publishes it below.
            let slot = self.pool.capture_slot(&id);
            let destination = slot.bytes_mut(info.byte_len)?;
            for row in 0..stream.height as usize {
                let source_start = row * source_stride;
                let destination_start = row * row_bytes;
                destination[destination_start..destination_start + row_bytes]
                    .copy_from_slice(&source[source_start..source_start + row_bytes]);
            }
            // The frame becomes ours only after the final source row reached
            // the pool. This timestamp makes C0→C1 include that required copy.
            info.captured_at = std::time::Instant::now();
            slot.set_info(info)?;
        }

        if let Err(id) = self.ready_slots.try_push(id) {
            // The slot-conservation invariant makes this impossible: removing
            // one ID from FreeSlots means ReadySlots cannot still hold all IDs.
            // Returning it to FreeSlots here would create a second producer.
            panic!("ReadySlots full after capture claimed {id:?}");
        }
        Ok(CapturePublish::Published)
    }
}

impl SenderPort {
    pub fn capture_is_running(&self) -> bool {
        self.capture_running.load(Ordering::Acquire)
    }

    /// Removes all currently ready IDs, releases stale frames, and retains the
    /// newest frame for the sender. FIFO removal is preserved even though only
    /// the final frame is transmitted.
    pub fn take_newest(&self) -> Option<SlotId> {
        let newest = self.ready_slots.try_pop()?;
        let mut newest = newest;

        while let Some(stale) = self.ready_slots.try_pop() {
            self.return_to_free(newest);
            self.counters
                .sender_dropped_stale_ready
                .fetch_add(1, Ordering::Relaxed);
            newest = stale;
        }

        Some(newest)
    }

    /// Runs sender work while this endpoint exclusively owns `id`.
    ///
    /// The closure cannot retain a reference to the slot after it returns.
    pub fn with_frame<R>(&self, id: &SlotId, operation: impl FnOnce(FrameInfo, &[u8]) -> R) -> R {
        unsafe {
            // SAFETY: take_newest removed `id` from ReadySlots. Only this
            // sender accesses it until the caller returns it with
            // return_to_free.
            let slot = self.pool.sender_slot(id);
            let info = slot
                .info()
                .expect("a ReadySlots ID must refer to completed frame metadata");
            let bytes = slot
                .bytes(info.byte_len)
                .expect("a ReadySlots frame length must fit its pool slot");
            operation(info, bytes)
        }
    }

    pub fn return_to_free(&self, id: SlotId) {
        unsafe {
            // SAFETY: sender owns `id` after take_newest, including when the
            // frame is dropped as stale or an error ends send processing.
            self.pool.clear_claimed_slot(&id);
        }
        self.free_slots
            .try_push(id)
            .expect("returning a sender-owned slot must not fill FreeSlots");
    }

    /// Returns and resets handoff outcomes since the previous report.
    pub fn take_snapshot(&self) -> HandoffSnapshot {
        HandoffSnapshot {
            capture_dropped_no_free_slot: self
                .counters
                .capture_dropped_no_free_slot
                .swap(0, Ordering::Relaxed),
            sender_dropped_stale_ready: self
                .counters
                .sender_dropped_stale_ready
                .swap(0, Ordering::Relaxed),
        }
    }
}

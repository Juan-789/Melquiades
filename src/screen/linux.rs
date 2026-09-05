//! Linux Wayland monitor capture through the XDG ScreenCast portal.
//!
//! `ashpd`/`zbus` form the permission control plane: the user chooses a
//! monitor and the portal returns a PipeWire node ID plus a restricted remote
//! file descriptor. PipeWire then owns the capture buffers. Its process
//! callback copies each BGRA frame once into our preallocated FramePool and
//! returns the PipeWire buffer immediately.

use std::error::Error;
use std::os::fd::OwnedFd;
use std::thread;
use std::time::Instant;

use ashpd::desktop::{
    PersistMode, Session,
    screencast::{
        CursorMode, OpenPipeWireRemoteOptions, Screencast, SelectSourcesOptions, SourceType,
    },
};
use pipewire::{
    context::ContextRc,
    core::CoreRc,
    main_loop::MainLoopRc,
    properties::properties,
    spa::{self, pod::Pod},
    stream::{StreamFlags, StreamListener, StreamRc},
};
use zbus::Connection;

use crate::capture::{FrameInfo, PixelFormat, StreamSpec};
use crate::pipeline::{CapturePort, CapturePublish, Pipeline};
use crate::transport::stream_from_sender;

/// The intentionally narrow first `cast` request: the portal lets the user
/// choose one full monitor, includes the pointer in the pixels, and grants no
/// persistent permission.
pub struct ShareScreen {
    source_type: SourceType,
    cursor_mode: CursorMode,
}

impl ShareScreen {
    pub const fn full_monitor() -> Self {
        Self {
            source_type: SourceType::Monitor,
            cursor_mode: CursorMode::Embedded,
        }
    }

    /// Requests one monitor, creates a PipeWire input stream for it, and sends
    /// captured frames to `receiver_addr` using the existing UDP sender.
    pub fn run(self, receiver_addr: &str) -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let authorized = runtime.block_on(self.authorize())?;
        let stream = authorized.stream_spec()?;
        let (capture, sender) = Pipeline::new(stream).into_ports();

        // PipeWire objects are deliberately !Send. Its event loop remains on
        // this thread while sender owns the other end of our lock-free handoff
        // on a normal Rust thread.
        let receiver_addr = receiver_addr.to_owned();
        let sender_addr = receiver_addr.clone();
        thread::Builder::new()
            .name("melquiades-screen-sender".into())
            .spawn(move || {
                if let Err(error) = stream_from_sender(sender, &sender_addr, stream) {
                    eprintln!("screen sender stopped: {error}");
                }
            })?;

        let remote = authorized.connect_pipewire(capture, stream)?;
        eprintln!(
            "PipeWire screen capture connected: node={} stream={}x{} {:?}; sending to {receiver_addr}",
            remote.node_id, stream.width, stream.height, stream.format,
        );
        eprintln!("press Ctrl-C to stop");
        remote.run();
        Ok(())
    }

    async fn authorize(self) -> Result<AuthorizedScreen, Box<dyn Error>> {
        // This is a direct zbus session-bus connection. ashpd wraps the portal
        // protocol on top of it; PipeWire is a separate connection below.
        let connection = Connection::session().await?;
        let portal = Screencast::with_connection(connection).await?;

        let available_sources = portal.available_source_types().await?;
        if !available_sources.contains(self.source_type) {
            return Err("the desktop portal cannot provide monitor capture".into());
        }
        let available_cursors = portal.available_cursor_modes().await?;
        if !available_cursors.contains(self.cursor_mode) {
            return Err("the desktop portal cannot embed the cursor".into());
        }

        let session = portal.create_session(Default::default()).await?;
        // ashpd represents the portal's `types` field as a bit-set even when
        // we intentionally request exactly one kind of source. OR-ing a flag
        // with itself constructs that one-element BitFlags set without adding
        // enumflags2 as a direct dependency.
        let requested_sources = self.source_type | self.source_type;
        portal
            .select_sources(
                &session,
                SelectSourcesOptions::default()
                    .set_sources(requested_sources)
                    .set_multiple(false)
                    .set_cursor_mode(self.cursor_mode)
                    .set_persist_mode(PersistMode::DoNot),
            )
            .await?;

        let response = portal
            .start(&session, None, Default::default())
            .await?
            .response()?;
        let streams = response.streams();
        if streams.len() != 1 {
            return Err(format!(
                "expected exactly one monitor stream, portal returned {}",
                streams.len()
            )
            .into());
        }
        let stream = &streams[0];
        let node_id = stream.pipe_wire_node_id();
        let compositor_size = stream.size();
        eprintln!(
            "portal selected monitor stream: node_id={node_id} compositor_size={compositor_size:?}"
        );

        let remote_fd = portal
            .open_pipe_wire_remote(&session, OpenPipeWireRemoteOptions::default())
            .await?;
        Ok(AuthorizedScreen {
            node_id,
            compositor_size,
            session,
            remote_fd,
        })
    }
}

/// A portal-authorized PipeWire endpoint. Keeping the session alive keeps its
/// monitor permission alive too.
struct AuthorizedScreen {
    node_id: u32,
    compositor_size: Option<(i32, i32)>,
    session: Session<Screencast>,
    remote_fd: OwnedFd,
}

impl AuthorizedScreen {
    fn stream_spec(&self) -> Result<StreamSpec, Box<dyn Error>> {
        let (width, height) = self
            .compositor_size
            .ok_or("portal did not report the selected monitor dimensions")?;
        let width = u32::try_from(width).map_err(|_| "portal reported a negative monitor width")?;
        let height =
            u32::try_from(height).map_err(|_| "portal reported a negative monitor height")?;
        StreamSpec::new(width, height, PixelFormat::Bgra8888)
    }

    fn connect_pipewire(
        self,
        capture: CapturePort,
        expected_stream: StreamSpec,
    ) -> Result<PipeWireRemote, Box<dyn Error>> {
        let Self {
            node_id,
            compositor_size: _,
            session,
            remote_fd,
        } = self;

        pipewire::init();
        let main_loop = MainLoopRc::new(None)?;
        let context = ContextRc::new(&main_loop, None)?;
        let core = context.connect_fd_rc(remote_fd, None)?;
        let stream = StreamRc::new(
            core.clone(),
            "melquiades-screen-capture",
            properties! {
                *pipewire::keys::MEDIA_TYPE => "Video",
                *pipewire::keys::MEDIA_CATEGORY => "Capture",
                *pipewire::keys::MEDIA_ROLE => "Screen",
            },
        )?;

        let listener = stream
            .add_local_listener_with_user_data(ScreenCaptureState::new(capture, expected_stream))
            .state_changed(|_, _, old, new| {
                eprintln!("PipeWire screen stream state: {old:?} -> {new:?}");
            })
            .param_changed(|_, state, id, param| {
                state.observe_format(id, param);
            })
            .process(|stream, state| {
                state.capture_one(stream);
            })
            .register()?;

        // `BGRx` has the byte order B, G, R, unused-X in memory. That is
        // compatible with our Bgra8888 consumer because it ignores alpha.
        // Asking for exactly this raw format avoids a later colorspace stage.
        let format = spa::pod::object!(
            spa::utils::SpaTypes::ObjectParamFormat,
            spa::param::ParamType::EnumFormat,
            spa::pod::property!(
                spa::param::format::FormatProperties::MediaType,
                Id,
                spa::param::format::MediaType::Video
            ),
            spa::pod::property!(
                spa::param::format::FormatProperties::MediaSubtype,
                Id,
                spa::param::format::MediaSubtype::Raw
            ),
            spa::pod::property!(
                spa::param::format::FormatProperties::VideoFormat,
                Id,
                spa::param::video::VideoFormat::BGRx
            ),
            spa::pod::property!(
                spa::param::format::FormatProperties::VideoSize,
                Rectangle,
                spa::utils::Rectangle {
                    width: expected_stream.width,
                    height: expected_stream.height,
                }
            ),
        );
        let bytes = spa::pod::serialize::PodSerializer::serialize(
            std::io::Cursor::new(Vec::new()),
            &spa::pod::Value::Object(format),
        )?
        .0
        .into_inner();
        let mut params = [Pod::from_bytes(&bytes).ok_or("failed to build PipeWire format pod")?];
        stream.connect(
            spa::utils::Direction::Input,
            Some(node_id),
            StreamFlags::AUTOCONNECT | StreamFlags::MAP_BUFFERS,
            &mut params,
        )?;

        Ok(PipeWireRemote {
            node_id,
            _session: session,
            _listener: listener,
            _stream: stream,
            _core: core,
            main_loop,
        })
    }
}

/// State owned by PipeWire's one capture callback. It never allocates or
/// waits: it either publishes one packed frame to the pool or drops it.
struct ScreenCaptureState {
    capture: CapturePort,
    expected_stream: StreamSpec,
    negotiated_format: bool,
    callbacks: u64,
    published: u64,
    malformed: u64,
    rejected_format: u64,
}

impl ScreenCaptureState {
    fn new(capture: CapturePort, expected_stream: StreamSpec) -> Self {
        Self {
            capture,
            expected_stream,
            negotiated_format: false,
            callbacks: 0,
            published: 0,
            malformed: 0,
            rejected_format: 0,
        }
    }

    fn observe_format(&mut self, id: u32, param: Option<&Pod>) {
        if id != spa::param::ParamType::Format.as_raw() {
            return;
        }
        let Some(param) = param else {
            self.negotiated_format = false;
            eprintln!("PipeWire cleared the screen stream format");
            return;
        };
        let Ok((media_type, media_subtype)) = spa::param::format_utils::parse_format(param) else {
            self.negotiated_format = false;
            eprintln!("PipeWire returned an unreadable screen format");
            return;
        };
        if media_type != spa::param::format::MediaType::Video
            || media_subtype != spa::param::format::MediaSubtype::Raw
        {
            self.negotiated_format = false;
            eprintln!("PipeWire did not negotiate raw video for the selected monitor");
            return;
        }

        let mut format = spa::param::video::VideoInfoRaw::new();
        if let Err(error) = format.parse(param) {
            self.negotiated_format = false;
            eprintln!("could not parse PipeWire screen format: {error}");
            return;
        }
        let size = format.size();
        self.negotiated_format = format.format() == spa::param::video::VideoFormat::BGRx
            && size.width == self.expected_stream.width
            && size.height == self.expected_stream.height;
        if self.negotiated_format {
            eprintln!(
                "PipeWire negotiated BGRx {}x{}; copying into tight BGRA pool slots",
                size.width, size.height
            );
        } else {
            eprintln!(
                "unsupported PipeWire screen format: {:?} {}x{}; expected BGRx {}x{}",
                format.format(),
                size.width,
                size.height,
                self.expected_stream.width,
                self.expected_stream.height,
            );
        }
    }

    fn capture_one(&mut self, stream: &pipewire::stream::Stream) {
        self.callbacks += 1;
        let capture_begins_at = Instant::now();
        let Some(mut buffer) = stream.dequeue_buffer() else {
            self.malformed += 1;
            return;
        };
        if !self.negotiated_format {
            self.rejected_format += 1;
            self.report_if_due();
            return;
        }
        let Some(data) = buffer.datas_mut().first_mut() else {
            self.malformed += 1;
            self.report_if_due();
            return;
        };

        let offset = data.chunk().offset() as usize;
        let chunk_size = data.chunk().size() as usize;
        let stride = data.chunk().stride();
        let Some(stride) = usize::try_from(stride).ok().filter(|stride| *stride > 0) else {
            self.malformed += 1;
            self.report_if_due();
            return;
        };
        let Some(memory) = data.data() else {
            // MAP_BUFFERS asked PipeWire to map CPU-readable buffers. A later
            // DMA-BUF path can handle this case without copying, but it is not
            // the first correct screen-capture implementation.
            self.malformed += 1;
            self.report_if_due();
            return;
        };
        let Some(end) = offset.checked_add(chunk_size) else {
            self.malformed += 1;
            self.report_if_due();
            return;
        };
        let Some(source) = memory.get(offset..end) else {
            self.malformed += 1;
            self.report_if_due();
            return;
        };

        let info = FrameInfo {
            capture_begins_at,
            width: self.expected_stream.width,
            height: self.expected_stream.height,
            format: PixelFormat::Bgra8888,
            byte_len: self.expected_stream.byte_len,
            captured_at: Instant::now(),
        };
        match self.capture.publish_strided(info, source, stride) {
            Ok(CapturePublish::Published) => self.published += 1,
            Ok(CapturePublish::DroppedNoFreeSlot) => {}
            Err(error) => {
                self.malformed += 1;
                if self.malformed == 1 {
                    eprintln!("screen frame rejected before pool publication: {error}");
                }
            }
        }
        // `buffer` drops here and queues the portal-owned PipeWire buffer
        // straight back to PipeWire. The FramePool holds our independent copy.
        self.report_if_due();
    }

    fn report_if_due(&self) {
        if self.callbacks > 0 && self.callbacks % 300 == 0 {
            eprintln!(
                "screen capture callbacks={} published={} malformed={} format_rejected={}",
                self.callbacks, self.published, self.malformed, self.rejected_format
            );
        }
    }
}

/// Owns the resources whose lifetimes must outlive the PipeWire stream
/// callback. Field order matters: listener unregisters before stream drops,
/// and stream keeps its core alive independently.
struct PipeWireRemote {
    node_id: u32,
    _session: Session<Screencast>,
    _listener: StreamListener<ScreenCaptureState>,
    _stream: StreamRc,
    _core: CoreRc,
    main_loop: MainLoopRc,
}

impl PipeWireRemote {
    fn run(&self) {
        self.main_loop.run();
    }
}

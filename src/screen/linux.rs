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
use crate::pipeline::{CapturePort, CapturePublish, Pipeline, SenderPort};
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
        let (remote, stream, _, sender) = authorized.connect_pipewire()?;

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

        eprintln!(
            "PipeWire screen capture connected: node={} stream={}x{} {:?}; sending to {receiver_addr}",
            remote.node_id, stream.width, stream.height, stream.format,
        );
        eprintln!("press Ctrl-C to stop");
        remote.run();
        Ok(())
    }

    pub(crate) async fn authorize(self) -> Result<AuthorizedScreen, Box<dyn Error>> {
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
            session,
            remote_fd,
        })
    }
}

/// A portal-authorized PipeWire endpoint. Keeping the session alive keeps its
/// monitor permission alive too.
pub(crate) struct AuthorizedScreen {
    node_id: u32,
    session: Session<Screencast>,
    remote_fd: OwnedFd,
}

impl AuthorizedScreen {
    pub(crate) fn connect_pipewire(
        self,
    ) -> Result<(PipeWireRemote, StreamSpec, CapturePort, SenderPort), Box<dyn Error>> {
        let Self {
            node_id,
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

        let loop_on_error = main_loop.clone();
        let loop_on_format = main_loop.clone();
        let (ready, negotiated) = std::sync::mpsc::channel();
        let listener = stream
            .add_local_listener_with_user_data(ScreenCaptureState::default())
            .state_changed(move |_, state, old, new| {
                eprintln!("PipeWire screen stream state: {old:?} -> {new:?}");
                if matches!(
                    new,
                    pipewire::stream::StreamState::Error(_)
                        | pipewire::stream::StreamState::Unconnected
                ) {
                    if let Some((_, capture)) = &state.output {
                        capture.stop();
                    }
                    loop_on_error.quit();
                }
            })
            .param_changed(move |_, state, id, param| {
                if id != spa::param::ParamType::Format.as_raw() {
                    return;
                }
                state.negotiated_format = false;
                let Some(param) = param else { return };
                let result = ScreenCaptureState::stream_spec(param);
                if let Some((previous, capture)) = &state.output {
                    state.negotiated_format = result.as_ref().ok() == Some(previous);
                    if !state.negotiated_format {
                        eprintln!("screen format changed, restart capture to select the new mode");
                        capture.stop();
                        loop_on_format.quit();
                    }
                    return;
                }
                let _ = ready.send(
                    result
                        .map(|spec| {
                            let (capture, sender) = Pipeline::new(spec).into_ports();
                            state.output = Some((spec, capture.clone()));
                            state.negotiated_format = true;
                            (spec, capture, sender)
                        })
                        .map_err(|e| e.to_string()),
                );
                loop_on_format.quit();
            })
            .process(|stream, state| {
                state.capture_one(stream);
            })
            .register()?;

        let format = |pixel_format: spa::param::video::VideoFormat| {
            spa::pod::object!(
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
                    pixel_format
                ),
            )
        };
        let serialize = |pixel_format| -> Result<Vec<u8>, Box<dyn Error>> {
            Ok(spa::pod::serialize::PodSerializer::serialize(
                std::io::Cursor::new(Vec::new()),
                &spa::pod::Value::Object(format(pixel_format)),
            )?
            .0
            .into_inner())
        };
        let bgrx = serialize(spa::param::video::VideoFormat::BGRx)?;
        let bgra = serialize(spa::param::video::VideoFormat::BGRA)?;
        let mut params = [
            Pod::from_bytes(&bgrx).ok_or("invalid bgrx format pod")?,
            Pod::from_bytes(&bgra).ok_or("invalid bgra format pod")?,
        ];
        stream.connect(
            spa::utils::Direction::Input,
            Some(node_id),
            StreamFlags::AUTOCONNECT | StreamFlags::MAP_BUFFERS,
            &mut params,
        )?;

        let loop_on_timeout = main_loop.clone();
        let timer = main_loop.loop_().add_timer(move |_| loop_on_timeout.quit());
        timer
            .update_timer(Some(std::time::Duration::from_secs(10)), None)
            .into_result()?;
        main_loop.run();
        drop(timer);
        let (spec, capture, sender) = negotiated.try_recv().map_err(|_| {
            format!(
                "pipewire ended format negotiation without a format ({:?})",
                stream.state()
            )
        })??;
        Ok((
            PipeWireRemote {
                node_id,
                _session: session,
                _listener: listener,
                _stream: stream,
                _core: core,
                main_loop,
            },
            spec,
            capture,
            sender,
        ))
    }
}

/// State owned by PipeWire's one capture callback. It never allocates or
/// waits: it either publishes one packed frame to the pool or drops it.
#[derive(Default)]
struct ScreenCaptureState {
    output: Option<(StreamSpec, CapturePort)>,
    negotiated_format: bool,
    callbacks: u64,
    published: u64,
    malformed: u64,
    rejected_format: u64,
}

impl ScreenCaptureState {
    fn stream_spec(param: &Pod) -> Result<StreamSpec, Box<dyn Error>> {
        let (media_type, media_subtype) = spa::param::format_utils::parse_format(param)
            .map_err(|e| format!("invalid pipewire format: {e:?}"))?;
        if media_type != spa::param::format::MediaType::Video
            || media_subtype != spa::param::format::MediaSubtype::Raw
        {
            return Err("pipewire did not negotiate raw video".into());
        }

        let mut format = spa::param::video::VideoInfoRaw::new();
        format
            .parse(param)
            .map_err(|e| format!("invalid video format: {e}"))?;
        let size = format.size();
        if !matches!(
            format.format(),
            spa::param::video::VideoFormat::BGRx | spa::param::video::VideoFormat::BGRA
        ) {
            return Err(format!("unsupported pipewire pixel format {:?}", format.format()).into());
        }
        StreamSpec::new(size.width, size.height, PixelFormat::Bgra8888)
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

        let Some((spec, capture)) = &self.output else {
            return;
        };
        let info = FrameInfo {
            capture_begins_at,
            width: spec.width,
            height: spec.height,
            format: spec.format,
            byte_len: spec.byte_len,
            captured_at: Instant::now(),
        };
        match capture.publish_strided(info, source, stride) {
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
pub(crate) struct PipeWireRemote {
    node_id: u32,
    _session: Session<Screencast>,
    _listener: StreamListener<ScreenCaptureState>,
    _stream: StreamRc,
    _core: CoreRc,
    main_loop: MainLoopRc,
}

impl PipeWireRemote {
    pub(crate) fn run_until(
        &self,
        should_stop: impl Fn() -> bool + 'static,
    ) -> Result<(), Box<dyn Error>> {
        let main_loop = self.main_loop.clone();
        let timer = self.main_loop.loop_().add_timer(move |_| {
            if should_stop() {
                main_loop.quit();
            }
        });
        timer
            .update_timer(
                Some(std::time::Duration::from_millis(100)),
                Some(std::time::Duration::from_millis(100)),
            )
            .into_result()?;
        self.main_loop.run();
        if let pipewire::stream::StreamState::Error(error) = self._stream.state() {
            return Err(format!("PipeWire capture failed: {error}").into());
        }
        Ok(())
    }

    fn run(&self) {
        self.main_loop.run();
    }
}

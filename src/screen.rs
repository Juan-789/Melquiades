//! Linux Wayland monitor-selection and PipeWire-remote setup.
//!
//! This is deliberately only the control-plane milestone. It proves that the
//! user selected one monitor and that PipeWire accepted the portal-authorized
//! file descriptor. The next step is to create a PipeWire `Stream`, negotiate
//! BGRA, and connect its process callback to `CapturePort`.

use std::error::Error;
use std::os::fd::OwnedFd;

use ashpd::desktop::{
    PersistMode, Session,
    screencast::{
        CursorMode, OpenPipeWireRemoteOptions, Screencast, SelectSourcesOptions, SourceType,
    },
};
use pipewire::{context::ContextRc, core::CoreRc, main_loop::MainLoopRc};
use zbus::Connection;

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

    /// Runs the portal-selection / restricted-PipeWire-remote probe.
    ///
    /// It stays alive in PipeWire's event loop after setup. Use Ctrl-C to end
    /// this probe; there is no pixel callback installed yet.
    pub fn run(self) -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let authorized = runtime.block_on(self.authorize())?;
        let remote = authorized.connect_pipewire()?;
        eprintln!(
            "PipeWire remote connected for monitor node {} (portal size {:?}).",
            remote.node_id, remote.compositor_size
        );
        eprintln!("screen capture callback is not installed yet; press Ctrl-C to exit");
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
    fn connect_pipewire(self) -> Result<PipeWireRemote, Box<dyn Error>> {
        let Self {
            node_id,
            compositor_size,
            session,
            remote_fd,
        } = self;
        let main_loop = MainLoopRc::new(None)?;
        let context = ContextRc::new(&main_loop, None)?;
        let core = context.connect_fd_rc(remote_fd, None)?;
        Ok(PipeWireRemote {
            node_id,
            compositor_size,
            _session: session,
            main_loop,
            _core: core,
        })
    }
}

/// Owns the resources whose lifetimes must outlive the future PipeWire stream
/// callback. `CoreRc` retains `ContextRc`; `ContextRc` retains its loop.
struct PipeWireRemote {
    node_id: u32,
    compositor_size: Option<(i32, i32)>,
    _session: Session<Screencast>,
    main_loop: MainLoopRc,
    _core: CoreRc,
}

impl PipeWireRemote {
    fn run(&self) {
        self.main_loop.run();
    }
}

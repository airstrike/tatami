//! In-process tatami-serve boot helper.
//!
//! [`Boot::run`] binds a runway server on `127.0.0.1:0` (kernel-assigned
//! port), wraps a [`tatami_inmem::InMemoryCube`] in a [`tatami_serve::Service`],
//! and returns both the running [`runway::Server`] handle and the actual
//! `http://127.0.0.1:<port>` URL the GUI's [`tatami_http::Remote`] will
//! point at.
//!
//! The auth secret is a 32-byte placeholder — same pattern the
//! `meridianre-serve` binary uses. The cube endpoints don't gate on
//! `require_user_id`, so the secret is never consulted.

use std::sync::Arc;

use anyhow::{Context, Result};
use runway::Module;
use runway::config::{Auth, Database, Server};
use tatami_inmem::InMemoryCube;

/// A running in-process tatami-serve instance and the base URL it listens on.
///
/// `Debug` is hand-rolled (rather than derived) because [`runway::server::Server`]
/// doesn't implement `Debug`; we render only the address and URL.
pub struct Boot {
    server: runway::server::Server,
    /// The base URL of the running server, e.g. `http://127.0.0.1:54321`.
    /// Held as a `String` so callers can hand it directly to
    /// [`tatami_http::Remote::new`] without pulling in a reqwest URL
    /// dependency at this layer.
    pub base_url: String,
}

impl std::fmt::Debug for Boot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Boot")
            .field("addr", &self.server.addr())
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl Boot {
    /// Bind the server on `127.0.0.1:0`, register the cube routes, and
    /// return once `runway::server::start` has acquired its listening
    /// socket. The kernel-assigned port is read out of the resulting
    /// [`runway::Server::addr`].
    pub async fn run(cube: InMemoryCube) -> Result<Self> {
        let service = tatami_serve::Service::new(Arc::new(cube));

        let mut router = runway::Router::new();
        service.routes(&mut router);

        let config = local_config();
        let server = runway::server::start(config, None, router.into_handle())
            .await
            .context("runway server failed to start")?;

        let base_url = format!("http://{}", server.addr());

        Ok(Self { server, base_url })
    }

    /// Gracefully shut down the in-process server.
    ///
    /// The board doesn't call this in v1 (the OS reclaims the port at
    /// process exit and dropping [`Boot`] closes the shutdown channel
    /// anyway), but the entry point is kept so callers wiring a clean
    /// teardown can opt in.
    #[allow(dead_code, reason = "exposed for callers that want explicit teardown")]
    pub async fn shutdown(self) -> Result<()> {
        self.server
            .shutdown()
            .await
            .context("runway server shutdown failed")
    }
}

/// Minimal runway config for the embedded server.
///
/// Port `0` lets the kernel pick a free port; `runway::server::start`
/// reads `local_addr()` after `bind()` so the actual port is observable
/// via [`runway::Server::addr`]. The 32-byte JWT secret is a placeholder
/// satisfying the `Auth` shape — the cube endpoints never consult it.
fn local_config() -> runway::Config {
    let server = Server {
        host: "127.0.0.1".to_owned(),
        port: 0,
        ..Server::default()
    };
    let database = Database::default();
    let auth = Auth {
        jwt_secret: "meridianre-board-local-dev-32-byt".to_owned(),
        ..Auth::default()
    };
    runway::Config::new(server, database, auth)
}

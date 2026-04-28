//! meridianre-serve — local development binary.
//!
//! Loads the meridianre `monthly_close.csv` into a Polars-backed
//! [`tatami_inmem::InMemoryCube`] and exposes it over HTTP via
//! [`tatami_serve::Service`] mounted on a [`runway`] server.
//!
//! Defaults bind `0.0.0.0:8080`; override via `--host`, `--port`, or the
//! `MERIDIANRE_HOST` / `MERIDIANRE_PORT` environment variables. The CSV
//! directory comes from `--data-dir` or `MERIDIANRE_DATA_DIR`, defaulting
//! to `~/inboard-ai/crates/meridianre/sample`.
//!
//! See `MAP_PHASE_L3.md` for the design rationale.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use runway::Module;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use meridianre_serve::cube;

/// CLI args. Each flag has a matching `MERIDIANRE_*` env var so the
/// binary fits naturally into shell-driven dev loops.
#[derive(Parser, Debug)]
#[command(name = "meridianre-serve", about = "Local meridianre cube server")]
struct Args {
    /// Directory containing `monthly_close.csv`.
    #[arg(
        long,
        env = "MERIDIANRE_DATA_DIR",
        default_value = "~/inboard-ai/crates/meridianre/sample"
    )]
    data_dir: String,

    /// Bind address.
    #[arg(long, env = "MERIDIANRE_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Bind port.
    #[arg(long, short = 'p', env = "MERIDIANRE_PORT", default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    let data_dir = expand_tilde(&args.data_dir);
    let csv_path = data_dir.join("monthly_close.csv");
    tracing::info!(path = %csv_path.display(), "loading meridianre fact table");

    let cube_inner = cube::build(&csv_path).context("building meridianre cube")?;
    tracing::info!("cube ready");

    let service = tatami_serve::Service::new(Arc::new(cube_inner));

    let mut router = runway::Router::new();
    service.routes(&mut router);

    let config = minimal_config(&args.host, args.port);
    tracing::info!(addr = %format!("{}:{}", args.host, args.port), "starting server");
    runway::server::run(config, None, router.into_handle())
        .await
        .context("runway server failed")
}

/// Tracing init that respects `RUST_LOG` if set, otherwise defaults to
/// `info` for our own crates and `warn` for hyper. Keeping the noisy
/// hyper-internal lines off the default surface so the boot trace stays
/// readable during the demo.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,hyper=warn".into());
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

/// Build a runway [`Config`](runway::Config) sufficient for the read-only
/// cube demo. Auth carries a placeholder HS256 secret because the cube
/// endpoints don't gate on `require_user_id` — the secret is never used,
/// it just needs to satisfy the `Config` shape (the runtime validation
/// lives in `Loader::load`, which we bypass).
fn minimal_config(host: &str, port: u16) -> runway::Config {
    use runway::config::{Auth, Database, Server};
    let server = Server {
        host: host.to_owned(),
        port,
        ..Server::default()
    };
    let database = Database::default();
    // 32 ASCII bytes — enough to satisfy any future loader-shaped check
    // without leaking a meaningful secret in source.
    let auth = Auth {
        jwt_secret: "meridianre-serve-local-dev-32-byt".to_owned(),
        ..Auth::default()
    };
    runway::Config::new(server, database, auth)
}

/// Expand a leading `~/` against `$HOME`. Avoids pulling in `dirs` for the
/// one substitution this binary needs.
fn expand_tilde(s: &str) -> PathBuf {
    if let Some(stripped) = s.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    PathBuf::from(s)
}

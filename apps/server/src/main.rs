//! Consumer engine server binary.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::serve;
use consumer_engine_core::EngineConfig;
use consumer_engine_server::Engine;
use tracing_subscriber::EnvFilter;

/// Entry point. Loads config (default or `--config <path>`), builds the engine,
/// and serves the REST API until Ctrl-C.
fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = load_config()?;
    let bind = config.bind.clone();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("tokio runtime")?;
    runtime.block_on(async move {
        // Engine::build spawns the compaction task via tokio::spawn, so it must
        // run inside the runtime.
        let (router, engine) = Engine::build(&config).context("build engine")?;
        let listener = tokio::net::TcpListener::bind(&bind)
            .await
            .with_context(|| format!("bind {bind}"))?;
        tracing::info!(%bind, "consumer engine listening");
        serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("serve")?;
        drop(engine);
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

/// Load configuration from `--config <path>` if given, else defaults.
fn load_config() -> Result<EngineConfig> {
    let args: Vec<String> = std::env::args().collect();
    let cfg_path = args
        .windows(2)
        .find(|w| w[0] == "--config")
        .map(|w| PathBuf::from(&w[1]));
    match cfg_path {
        Some(p) => EngineConfig::from_yaml_file(&p).context("load config"),
        None => Ok(EngineConfig::default()),
    }
}

/// Wait for Ctrl-C / SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install terminate handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

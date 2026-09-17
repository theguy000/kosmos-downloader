#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use kosmos_downloader::engine::DownloadEngine;
use kosmos_downloader::ui::run_app;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let _runtime_guard = runtime.enter();

    let engine = DownloadEngine::new();
    let action_tx = engine.action_tx();
    let snapshot_rx = engine.snapshot_rx();

    run_app(action_tx, snapshot_rx)?;

    Ok(())
}

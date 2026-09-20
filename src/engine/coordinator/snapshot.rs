use super::scheduler::rebalance_workers;
use super::{Session, calculate_downloaded, uses_range_workers};
use crate::engine::model::{DownloadSnapshot, DownloadStatus};
use std::time::Instant;

impl Session {
    pub(super) fn tick(&mut self) {
        if self.status == DownloadStatus::Downloading {
            if let (Some(info), Some(storage)) = (&self.file_info, &self.active_storage)
                && let Err(error) = rebalance_workers(
                    &mut self.active_chunks,
                    &mut self.worker_handles,
                    self.current_num_chunks,
                    self.session_id,
                    &self.current_url,
                    &self.client,
                    info,
                    storage,
                    &self.cancel_tx,
                    &self.worker_tx,
                )
            {
                let _ = self.cancel_tx.send(true);
                self.status = DownloadStatus::Failed(error.to_string());
                self.current_speed = 0;
                self.publish(
                    info.content_length,
                    calculate_downloaded(&self.active_chunks),
                    0,
                    None,
                    uses_range_workers(info),
                );
                return;
            }
            let now = Instant::now();
            let elapsed = now.duration_since(self.last_tick).as_secs_f64();
            self.last_tick = now;

            if elapsed > 0.05 {
                let instant_speed = (self.bytes_since_last_tick as f64 / elapsed) as u64;
                self.bytes_since_last_tick = 0;

                // Exponential moving average for smooth speed display
                self.current_speed = if self.current_speed == 0 {
                    instant_speed
                } else {
                    ((self.current_speed as f64 * 0.7) + (instant_speed as f64 * 0.3)) as u64
                };
            }

            let downloaded = calculate_downloaded(&self.active_chunks);
            let total_bytes = self.file_info.as_ref().and_then(|i| i.content_length);

            let eta_seconds = if let Some(total) = total_bytes {
                if self.current_speed > 0 && total > downloaded {
                    Some((total - downloaded) / self.current_speed)
                } else {
                    None
                }
            } else {
                None
            };

            let resumable = self.file_info.as_ref().is_some_and(uses_range_workers);

            self.publish(
                total_bytes,
                downloaded,
                self.current_speed,
                eta_seconds,
                resumable,
            );
        }
    }

    pub(super) fn publish(
        &self,
        total_bytes: Option<u64>,
        downloaded_bytes: u64,
        speed_bytes_per_sec: u64,
        eta_seconds: Option<u64>,
        resumable: bool,
    ) {
        let snapshot = DownloadSnapshot {
            session_id: self.session_id,
            url: self.current_url.clone(),
            filename: self.current_filename.clone(),
            save_path: self.current_path.clone(),
            status: self.status.clone(),
            total_bytes,
            downloaded_bytes,
            speed_bytes_per_sec,
            eta_seconds,
            resumable,
        };
        let _ = self.snapshot_tx.send(snapshot);
    }
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_speed(bps: u64) -> String {
    if bps == 0 {
        "0 KB/s".to_string()
    } else {
        format!("{}/s", format_bytes(bps))
    }
}

pub fn format_eta(eta_seconds: Option<u64>) -> String {
    match eta_seconds {
        None => "--:--".to_string(),
        Some(s) if s >= 3600 => {
            let hours = s / 3600;
            let mins = (s % 3600) / 60;
            format!("{hours}h {mins}m")
        }
        Some(s) if s >= 60 => {
            let mins = s / 60;
            let secs = s % 60;
            format!("{mins}m {secs}s")
        }
        Some(s) => format!("{s}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_format_eta() {
        assert_eq!(format_eta(None), "--:--");
        assert_eq!(format_eta(Some(12)), "12s");
        assert_eq!(format_eta(Some(75)), "1m 15s");
        assert_eq!(format_eta(Some(3665)), "1h 1m");
    }
}

/// Extracts and sanitizes the suggested filename from Content-Disposition or URL path.
pub fn extract_filename(url_str: &str, content_disposition: Option<&str>) -> String {
    // 1. Try Content-Disposition header
    if let Some(disposition) = content_disposition {
        // e.g. filename*=utf-8''encoded_name.ext
        if let Some(idx) = disposition.to_ascii_lowercase().find("filename*=") {
            let value = &disposition[idx + 10..].trim();
            let raw_name = value.split(';').next().unwrap_or(value).trim();
            let name_part = if let Some(last_quote) = raw_name.rfind("''") {
                &raw_name[last_quote + 2..]
            } else {
                raw_name
            };
            let cleaned = name_part.trim_matches('"').trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }

        // e.g. filename="name.ext" or filename=name.ext
        if let Some(idx) = disposition.to_ascii_lowercase().find("filename=") {
            let value = &disposition[idx + 9..].trim();
            let raw_name = value.split(';').next().unwrap_or(value).trim();
            let cleaned = raw_name.trim_matches('"').trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }
    }

    // 2. Extract from URL path
    if let Ok(parsed) = reqwest::Url::parse(url_str) {
        let path = parsed.path();
        if let Some(segment) = path.split('/').rfind(|s| !s.is_empty()) {
            let cleaned = segment.trim();
            if !cleaned.is_empty() {
                return sanitize_filename(cleaned);
            }
        }
    }

    "download.bin".to_string()
}

/// Replaces characters that are illegal in file systems with underscores.
fn sanitize_filename(name: &str) -> String {
    let decoded = urlencoding_decode(name);
    let mut sanitized = String::with_capacity(decoded.len());

    for ch in decoded.chars() {
        match ch {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => sanitized.push('_'),
            c if c.is_control() => sanitized.push('_'),
            c => sanitized.push(c),
        }
    }

    let trimmed = sanitized.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        return "download.bin".to_string();
    }

    // Guard against Windows reserved device names (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
    let upper = trimmed.to_ascii_uppercase();
    let stem = upper.split('.').next().unwrap_or(&upper);
    const RESERVED: &[&str] = &[
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    if RESERVED.contains(&stem) {
        format!("_{trimmed}")
    } else {
        trimmed.to_string()
    }
}

/// Simple percent-decode helper without pulling extra dependencies.
fn urlencoding_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut chars = s.bytes();

    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next();
            let h2 = chars.next();
            if let (Some(c1), Some(c2)) = (h1, h2) {
                let hex_str = [c1, c2];
                if let Ok(hex_val) = std::str::from_utf8(&hex_str)
                    && let Ok(byte) = u8::from_str_radix(hex_val, 16)
                {
                    bytes.push(byte);
                    continue;
                }
                bytes.push(b'%');
                bytes.push(c1);
                bytes.push(c2);
            } else {
                bytes.push(b'%');
                if let Some(c1) = h1 {
                    bytes.push(c1);
                }
            }
        } else {
            bytes.push(b);
        }
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::extract_filename;

    #[test]
    fn test_extract_filename_from_url() {
        assert_eq!(
            extract_filename("https://example.com/files/archive.zip", None),
            "archive.zip"
        );
        assert_eq!(
            extract_filename("https://example.com/downloads/setup.exe?key=123", None),
            "setup.exe"
        );
        assert_eq!(
            extract_filename("https://example.com/spaced%20file%20name.pdf", None),
            "spaced file name.pdf"
        );
    }

    #[test]
    fn test_extract_filename_content_disposition() {
        let disp = "attachment; filename=\"report_2026.docx\"";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp)),
            "report_2026.docx"
        );

        let disp_unquoted = "attachment; filename=image.png; size=1234";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp_unquoted)),
            "image.png"
        );

        let disp_utf8 = "attachment; filename*=UTF-8''my%20data%20sheet.csv";
        assert_eq!(
            extract_filename("https://example.com/get", Some(disp_utf8)),
            "my data sheet.csv"
        );
    }

    #[test]
    fn test_extract_filename_sanitization() {
        let disp = "attachment; filename=\"bad:file*name?.iso\"";
        assert_eq!(
            extract_filename("https://example.com/test", Some(disp)),
            "bad_file_name_.iso"
        );
    }

    #[test]
    fn test_extract_filename_trailing_slash_and_reserved_names() {
        assert_eq!(
            extract_filename("https://example.com/files/archive.tar.gz/", None),
            "archive.tar.gz"
        );
        assert_eq!(extract_filename("https://example.com/nul", None), "_nul");
        assert_eq!(
            extract_filename("https://example.com/con.txt", None),
            "_con.txt"
        );
    }
}

use std::path::{Path, PathBuf};

/// Opens `path` with the system handler, ignoring files that were moved or removed.
pub(super) fn open_file(path: &Path) {
    if !path.exists() {
        return;
    }

    if let Some(path_str) = path.to_str() {
        let _ = std::process::Command::new("cmd")
            .args(["/c", "start", "", path_str])
            .spawn();
    }
}

pub(super) fn default_download_directory() -> PathBuf {
    if let Ok(userprofile) = std::env::var("USERPROFILE") {
        let p = PathBuf::from(userprofile).join("Downloads");
        if p.exists() {
            return p;
        }
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

#[cfg(windows)]
const RUN_KEY: &str = r"HKCU\Software\Microsoft\Windows\CurrentVersion\Run";
#[cfg(windows)]
const VALUE_NAME: &str = "Kosmos Downloader";
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

/// Whether Kosmos Downloader is registered to launch at user sign-in.
#[cfg(windows)]
pub(super) fn startup_enabled() -> bool {
    startup_enabled_in(RUN_KEY, VALUE_NAME)
}

/// Adds or removes the current executable from the user's sign-in launch list.
#[cfg(windows)]
pub(super) fn set_startup_enabled(enabled: bool) -> std::io::Result<()> {
    let executable = std::env::current_exe()?;
    let value = enabled.then(|| startup_value(&executable));
    set_startup_in(RUN_KEY, VALUE_NAME, value.as_deref())
}

#[cfg(not(windows))]
pub(super) const fn startup_enabled() -> bool {
    false
}

#[cfg(not(windows))]
pub(super) fn set_startup_enabled(_enabled: bool) -> std::io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn startup_value(executable: &Path) -> String {
    // Windows splits the Run command line on spaces unless the path is quoted.
    format!("\"{}\"", executable.display())
}

// ponytail: presence is the whole state; parsing localized `reg query` output is fragile.
#[cfg(windows)]
fn startup_enabled_in(key: &str, value_name: &str) -> bool {
    reg(&["query", key, "/v", value_name]).is_ok_and(|status| status.success())
}

#[cfg(windows)]
fn set_startup_in(key: &str, value_name: &str, value: Option<&str>) -> std::io::Result<()> {
    if value.is_none() && !startup_enabled_in(key, value_name) {
        return Ok(());
    }
    let status = match value {
        Some(value) => reg(&[
            "add", key, "/v", value_name, "/t", "REG_SZ", "/d", value, "/f",
        ])?,
        None => reg(&["delete", key, "/v", value_name, "/f"])?,
    };
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "reg.exe exited with {status}"
        )))
    }
}

#[cfg(windows)]
fn reg(arguments: &[&str]) -> std::io::Result<std::process::ExitStatus> {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    Command::new(reg_executable()?)
        .args(arguments)
        .creation_flags(CREATE_NO_WINDOW)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
}

#[cfg(windows)]
fn reg_executable() -> std::io::Result<PathBuf> {
    let root = std::env::var_os("SystemRoot")
        .filter(|root| !root.is_empty())
        .ok_or_else(|| std::io::Error::other("SystemRoot is not set"))?;
    Ok(PathBuf::from(root).join("System32").join("reg.exe"))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn test_startup_value_quotes_paths_with_spaces() {
        assert_eq!(
            startup_value(Path::new(r"C:\Program Files\Kosmos\app.exe")),
            r#""C:\Program Files\Kosmos\app.exe""#
        );
    }

    #[test]
    fn test_startup_registration_round_trip() {
        let key = format!(
            r"HKCU\Software\KosmosDownloader\tests\{}",
            std::process::id()
        );
        let value_name = "Kosmos Downloader Test";
        let executable = std::env::current_exe().expect("test executable path");
        let value = startup_value(&executable);

        assert!(!startup_enabled_in(&key, value_name));

        set_startup_in(&key, value_name, Some(&value)).expect("register test startup value");
        assert!(startup_enabled_in(&key, value_name));

        set_startup_in(&key, value_name, None).expect("remove test startup value");
        assert!(!startup_enabled_in(&key, value_name));

        // Disabling an already-disabled value is a no-op, not an error.
        set_startup_in(&key, value_name, None).expect("repeat disable is idempotent");

        // Best-effort cleanup of the throwaway parent key; the value under test is already gone.
        let _ = reg(&["delete", r"HKCU\Software\KosmosDownloader", "/f"]);
    }
}

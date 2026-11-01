use std::{path::PathBuf, time::Duration};

/// The env path is a gate; `<gate>.paused` signals arrival, and removing the gate resumes.
pub(crate) async fn pause(env_var: &str) {
    let Some(gate) = std::env::var_os(env_var).map(PathBuf::from) else {
        return;
    };
    if !gate.exists() {
        return;
    }

    let mut paused_name = gate.as_os_str().to_owned();
    paused_name.push(".paused");
    let paused = PathBuf::from(paused_name);
    if let Err(error) = std::fs::write(&paused, b"paused") {
        tracing::warn!(hook = env_var, %error, "test pause hook could not signal arrival");
        return;
    }

    while gate.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = std::fs::remove_file(paused);
}

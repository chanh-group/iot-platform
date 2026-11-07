use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

#[test]
fn load_runs_use_separate_temporary_directories() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let cargo = temp.path().join("cargo");
    std::fs::write(&cargo, "#!/bin/sh\nprintf 'TMP=%s\\n' \"$TMPDIR\"\n")?;
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755))?;
    let path = std::env::join_paths(std::iter::once(temp.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/load/run.sh");
    let start = || {
        Command::new("sh")
            .arg(&script)
            .env("PATH", &path)
            .env("IOT_LOAD_TMPDIR", temp.path().join("runs"))
            .env("IOT_LOAD_CGROUP", "0")
            .stdout(std::process::Stdio::piped())
            .spawn()
    };
    let first = start()?;
    let second = start()?;
    let first = first.wait_with_output()?;
    let second = second.wait_with_output()?;
    anyhow::ensure!(first.status.success() && second.status.success());
    let first = String::from_utf8(first.stdout)?;
    let second = String::from_utf8(second.stdout)?;
    let directory = |output: &str| {
        output
            .lines()
            .find_map(|line| line.strip_prefix("TMP="))
            .map(str::to_owned)
    };
    let first = directory(&first).ok_or_else(|| anyhow::anyhow!("first TMPDIR missing"))?;
    let second = directory(&second).ok_or_else(|| anyhow::anyhow!("second TMPDIR missing"))?;
    anyhow::ensure!(first != second, "load runs shared a directory");
    anyhow::ensure!(!Path::new(&first).exists() && !Path::new(&second).exists());
    Ok(())
}

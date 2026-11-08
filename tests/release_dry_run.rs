use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[test]
fn dry_run_refuses_existing_out_dir_and_keeps_its_contents() {
    let out = tempfile::tempdir().expect("temp dir");
    let sentinel = out.path().join("keep.txt");
    fs::write(&sentinel, b"keep").expect("write sentinel");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/release-dry-run.sh");

    let output = Command::new("sh")
        .arg(script)
        .env("OUT_DIR", out.path())
        .output()
        .expect("run release dry-run guard");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("refusing to reuse existing OUT_DIR"),
        "guard did not run: {stderr}"
    );
    assert_eq!(
        fs::read(&sentinel).expect("sentinel remains"),
        b"keep".as_slice()
    );
}

#[cfg(unix)]
#[test]
fn dry_run_refuses_existing_image_before_creating_output() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("temp dir");
    let docker = dir.path().join("docker");
    fs::write(
        &docker,
        "#!/bin/sh\n[ \"$1\" = image ] && [ \"$2\" = inspect ] && [ \"$3\" = iot-server:already-exists ]\n",
    )
    .expect("write fake docker");
    fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).expect("chmod docker");
    let path = std::env::join_paths(std::iter::once(dir.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .expect("PATH");
    let out = dir.path().join("new-output");
    let output = Command::new("sh")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/release-dry-run.sh"))
        .env("PATH", path)
        .env("OUT_DIR", &out)
        .env("IOT_RELEASE_IMAGE", "iot-server:already-exists")
        .output()
        .expect("run image guard");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("refusing to overwrite existing image")
    );
    assert!(!out.exists());
}

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

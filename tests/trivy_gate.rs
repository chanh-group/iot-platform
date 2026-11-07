use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

fn run_gate(report: &str, exceptions: &str) -> Output {
    let dir = tempfile::tempdir().expect("temp dir");
    let report_path = dir.path().join("report.json");
    let exceptions_path = dir.path().join("exceptions.json");
    fs::write(&report_path, report).expect("write report");
    fs::write(&exceptions_path, exceptions).expect("write exceptions");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deploy/check-trivy-report.py");

    Command::new("python3")
        .arg(script)
        .arg(&report_path)
        .arg(&exceptions_path)
        .output()
        .expect("run trivy gate")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn report(findings: &str) -> String {
    format!(
        r#"{{"SchemaVersion":2,"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{{"OS":{{"Family":"ubuntu","Name":"24.04"}}}},"Results":[{{"Target":"iot-server:test (ubuntu 24.04)","Class":"os-pkgs","Type":"ubuntu","Vulnerabilities":[{findings}]}}]}}"#
    )
}

fn clean_report() -> &'static str {
    r#"{"SchemaVersion":2,"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"iot-server:test (ubuntu 24.04)","Class":"os-pkgs","Type":"ubuntu"}]}"#
}

fn finding(id: &str, package: &str, severity: &str) -> String {
    format!(
        r#"{{"VulnerabilityID":"{id}","PkgName":"{package}","InstalledVersion":"1.0","Severity":"{severity}"}}"#
    )
}

fn exception(id: &str, package: &str, owner: &str, expires: &str) -> String {
    format!(
        r#"{{"id":"{id}","package":"{package}","owner":"{owner}","reason":"reason","mitigation":"mitigation","expires":"{expires}"}}"#
    )
}

#[test]
fn matching_exceptions_pass_for_high_and_critical() {
    let report = report(&format!(
        "{},{}",
        finding("CVE-2026-0001", "zlib1g", "HIGH"),
        finding("CVE-2026-0002", "libssl3", "CRITICAL")
    ));
    let exceptions = format!(
        r#"{{"exceptions":[{},{}]}}"#,
        exception("CVE-2026-0001", "zlib1g", "maintainer", "2999-12-31"),
        exception("CVE-2026-0002", "libssl3", "maintainer", "2999-12-31")
    );

    let output = run_gate(&report, &exceptions);

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).contains("Trivy gate OK"));
}

#[test]
fn missing_owner_is_rejected() {
    let report = report(&finding("CVE-2026-0001", "zlib1g", "HIGH"));
    let exceptions = format!(
        r#"{{"exceptions":[{}]}}"#,
        exception("CVE-2026-0001", "zlib1g", "", "2999-12-31")
    );

    let output = run_gate(&report, &exceptions);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("'owner'"));
}

#[test]
fn expired_exception_is_rejected() {
    let report = report(&finding("CVE-2026-0001", "zlib1g", "HIGH"));
    let exceptions = format!(
        r#"{{"exceptions":[{}]}}"#,
        exception("CVE-2026-0001", "zlib1g", "maintainer", "2000-01-01")
    );

    let output = run_gate(&report, &exceptions);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("expired"));
}

#[test]
fn unapproved_finding_is_rejected() {
    let report = report(&finding("CVE-2026-0001", "zlib1g", "CRITICAL"));

    let output = run_gate(&report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("unapproved"));
    assert!(stderr.contains("CVE-2026-0001"));
}

#[test]
fn stale_exception_is_rejected() {
    let exceptions = format!(
        r#"{{"exceptions":[{}]}}"#,
        exception("CVE-2026-0001", "zlib1g", "maintainer", "2999-12-31")
    );

    let output = run_gate(clean_report(), &exceptions);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("match no current"));
}

#[test]
fn clean_report_without_exceptions_passes() {
    let output = run_gate(clean_report(), r#"{"exceptions":[]}"#);

    assert!(output.status.success(), "{}", stderr(&output));
}

#[test]
fn empty_report_is_rejected() {
    let output = run_gate("{}", r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("ArtifactType"));
}

#[test]
fn missing_metadata_is_rejected() {
    let report = r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Results":[{"Target":"x"}]}"#;

    let output = run_gate(report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("Metadata"));
}

#[test]
fn non_image_report_is_rejected() {
    let report = r#"{"ArtifactName":"src","ArtifactType":"filesystem","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"src","Class":"lang-pkgs","Type":"cargo"}]}"#;

    let output = run_gate(report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("container image"));
}

#[test]
fn empty_result_is_rejected() {
    let report = r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{}]}"#;

    let output = run_gate(report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("Target"));
}

#[test]
fn missing_os_scan_result_is_rejected() {
    let report = r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"app","Class":"lang-pkgs","Type":"cargo"}]}"#;

    let output = run_gate(report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("os-pkgs"));
}

#[test]
fn mismatched_os_result_is_rejected() {
    let report = r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"iot-server:test (debian 12)","Class":"os-pkgs","Type":"debian"}]}"#;

    let output = run_gate(report, r#"{"exceptions":[]}"#);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("does not match OS family"));
}

#[test]
fn malformed_results_are_rejected() {
    let cases = [
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}}}"#,
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":{}}"#,
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[]}"#,
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[42]}"#,
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"x","Class":"os-pkgs","Type":"ubuntu","Vulnerabilities":42}]}"#,
        r#"{"ArtifactName":"iot-server:test","ArtifactType":"container_image","Metadata":{"OS":{"Family":"ubuntu","Name":"24.04"}},"Results":[{"Target":"x","Class":"os-pkgs","Type":"ubuntu","Vulnerabilities":[42]}]}"#,
    ];

    for report in cases {
        let output = run_gate(report, r#"{"exceptions":[]}"#);
        assert!(
            !output.status.success(),
            "malformed report accepted: {report}\n{}",
            stderr(&output)
        );
    }
}

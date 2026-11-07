#!/usr/bin/env python3
"""Validate release exceptions and enforce the Trivy HIGH/CRITICAL gate."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REQUIRED_FIELDS = ("id", "package", "owner", "reason", "mitigation", "expires")
SEVERITIES = {"HIGH", "CRITICAL"}
KNOWN_SEVERITIES = {"UNKNOWN", "LOW", "MEDIUM", "HIGH", "CRITICAL"}


def fail(message: str) -> None:
    print(f"trivy gate: {message}", file=sys.stderr)
    raise SystemExit(1)


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def load_exceptions(document) -> dict[tuple[str, str], dict]:
    if not isinstance(document, dict) or not isinstance(document.get("exceptions"), list):
        fail("exception file must be an object with an 'exceptions' array")
    today = datetime.now(timezone.utc).date()
    exceptions = {}
    for index, entry in enumerate(document["exceptions"]):
        if not isinstance(entry, dict):
            fail(f"exception #{index} must be an object")
        for field in REQUIRED_FIELDS:
            value = entry.get(field)
            if not isinstance(value, str) or not value.strip():
                fail(f"exception #{index} missing non-empty '{field}'")
        try:
            expires = datetime.strptime(entry["expires"], "%Y-%m-%d").date()
        except ValueError:
            fail(f"exception #{index} has invalid 'expires' (expected YYYY-MM-DD)")
        if expires < today:
            fail(f"exception #{index} expired on {entry['expires']}")
        key = (entry["id"], entry["package"])
        if key in exceptions:
            fail(f"duplicate exception for {entry['id']} / {entry['package']}")
        exceptions[key] = entry
    return exceptions


def non_empty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def load_findings(report) -> dict[tuple[str, str], dict]:
    if not isinstance(report, dict):
        fail("Trivy report must be an object")
    if report.get("ArtifactType") != "container_image":
        fail("Trivy report is not a container image scan ('ArtifactType')")
    if not non_empty_string(report.get("ArtifactName")):
        fail("Trivy report missing non-empty 'ArtifactName'")
    metadata = report.get("Metadata")
    if not isinstance(metadata, dict):
        fail("Trivy report missing 'Metadata' object")
    os_info = metadata.get("OS")
    if not isinstance(os_info, dict) or not non_empty_string(os_info.get("Family")):
        fail("Trivy report missing 'Metadata.OS.Family'")
    os_family = os_info["Family"]

    results = report.get("Results")
    if not isinstance(results, list):
        fail("Trivy report missing 'Results' array")
    if not results:
        fail("Trivy report has empty 'Results'; refusing to treat it as clean")

    findings = {}
    os_results = 0
    for result_index, result in enumerate(results):
        if not isinstance(result, dict):
            fail(f"Trivy result #{result_index} must be an object")
        if not non_empty_string(result.get("Target")):
            fail(f"Trivy result #{result_index} missing non-empty 'Target'")
        result_class = result.get("Class")
        if not non_empty_string(result_class):
            fail(f"Trivy result #{result_index} missing non-empty 'Class'")
        result_type = result.get("Type")
        if not non_empty_string(result_type):
            fail(f"Trivy result #{result_index} missing non-empty 'Type'")
        if result_class == "os-pkgs":
            if result_type.lower() != os_family.lower():
                fail(
                    f"Trivy os-pkgs result #{result_index} type '{result_type}' "
                    f"does not match OS family '{os_family}'"
                )
            os_results += 1
        vulnerabilities = result.get("Vulnerabilities", [])
        if not isinstance(vulnerabilities, list):
            fail(f"Trivy result #{result_index} 'Vulnerabilities' must be an array")
        for finding_index, finding in enumerate(vulnerabilities):
            if not isinstance(finding, dict):
                fail(f"Trivy finding #{result_index}/{finding_index} must be an object")
            severity = finding.get("Severity")
            if severity not in KNOWN_SEVERITIES:
                fail(f"Trivy finding #{result_index}/{finding_index} has invalid 'Severity'")
            if severity not in SEVERITIES:
                continue
            if not non_empty_string(finding.get("VulnerabilityID")) or not non_empty_string(
                finding.get("PkgName")
            ):
                fail(
                    f"Trivy finding #{result_index}/{finding_index} missing "
                    "'VulnerabilityID' or 'PkgName'"
                )
            findings[(finding["VulnerabilityID"], finding["PkgName"])] = finding
    if os_results == 0:
        fail(f"Trivy report has no 'os-pkgs' result for OS family '{os_family}'")
    return findings


def main() -> int:
    if len(sys.argv) != 3:
        fail("usage: check-trivy-report.py <trivy.json> <exceptions.json>")
    exceptions = load_exceptions(load_json(Path(sys.argv[2])))
    findings = load_findings(load_json(Path(sys.argv[1])))

    stale = sorted(set(exceptions) - set(findings))
    if stale:
        names = ", ".join(f"{vulnerability}/{package}" for vulnerability, package in stale)
        fail(f"exception(s) match no current HIGH/CRITICAL finding: {names}")

    unapproved = [finding for key, finding in findings.items() if key not in exceptions]
    if unapproved:
        for finding in unapproved:
            print(
                f"unapproved {finding.get('Severity')} {finding.get('VulnerabilityID')} "
                f"{finding.get('PkgName')} {finding.get('InstalledVersion')}",
                file=sys.stderr,
            )
        fail(f"{len(unapproved)} unapproved HIGH/CRITICAL finding(s)")

    print(
        f"Trivy gate OK: {len(findings)} HIGH/CRITICAL finding(s), "
        f"{len(exceptions)} approved exception(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

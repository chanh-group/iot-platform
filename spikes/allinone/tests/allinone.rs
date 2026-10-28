#[tokio::test]
async fn all_in_one_stack() {
    let report = allinone_spike::self_check()
        .await
        .expect("all-in-one self check");
    assert_eq!(report.journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(report.synchronous, 2);
    assert_eq!(report.foreign_keys, 1);
    assert_eq!(report.migrations, 1);
    assert_eq!(report.devices, 1);
    assert_eq!(report.ws_echo, "ping");
    assert_eq!(report.mqtt_payload, "from-server");
}

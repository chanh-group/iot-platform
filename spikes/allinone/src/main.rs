#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let report = allinone_spike::self_check().await?;
    println!(
        "allinone ok: http={} mqtt={} journal={} synchronous={} fk={} migrations={} devices={} ws={} mqtt_payload={}",
        report.http_addr,
        report.mqtt_addr,
        report.journal_mode,
        report.synchronous,
        report.foreign_keys,
        report.migrations,
        report.devices,
        report.ws_echo,
        report.mqtt_payload,
    );
    Ok(())
}

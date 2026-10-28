use std::collections::HashMap;
use std::time::Duration;

use rmqtt_spike::{SpikeConfig, SpikeServer};

const DEVICE_A: &str = "019c0e1e-0000-7000-8000-00000000000a";
const DEVICE_B: &str = "019c0e1e-0000-7000-8000-00000000000b";
const TOKEN_A: &str = "iot_spike_token_for_device_a_000000000000";
const TOKEN_B: &str = "iot_spike_token_for_device_b_000000000000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let devices = HashMap::from([
        (DEVICE_A.to_owned(), TOKEN_A.to_owned()),
        (DEVICE_B.to_owned(), TOKEN_B.to_owned()),
    ]);
    let config = SpikeConfig {
        devices,
        session_expiry: Duration::from_secs(3600),
        ..SpikeConfig::default()
    };
    let server = SpikeServer::start_on("127.0.0.1:18883".parse()?, config).await?;
    println!("spike listening on {}", server.addr());
    println!("device A: {DEVICE_A} / {TOKEN_A}");
    println!("device B: {DEVICE_B} / {TOKEN_B}");
    println!("press Ctrl-C to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}

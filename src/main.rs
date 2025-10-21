mod election;

use std::time::Duration;

use clap::Parser;
use election::{Election, ElectionConfig};
use etcd_client::Client;
use simple_logger::SimpleLogger;

#[derive(clap::Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(long, default_value_t = false, help = "Set logger to debug mode")]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    let log_level = if args.verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Error
    };
    SimpleLogger::new().with_level(log_level).init()?;

    app(Config {
        advertise_host: String::from("node-01-proxy"),
        advertise_port: 80,
        leader_key: String::from("etcd-reverse-proxy/leader"),
        etcd_addr: vec![String::from("node-01-etcd:2379")],
    })
    .await?;

    Ok(())
}

struct Config {
    advertise_host: String,
    advertise_port: u16,
    leader_key: String,
    etcd_addr: Vec<String>,
}

async fn app(config: Config) -> anyhow::Result<()> {
    let client = Client::connect(config.etcd_addr, None).await?;

    let instance_id = format!("{}:{}", config.advertise_host, config.advertise_port);
    let election_config = ElectionConfig {
        leader_key: config.leader_key,
        ttl_second: 60,
        timeout: Duration::from_secs(30),
    };
    let mut election = Election::new(instance_id, client, election_config);
    election.run().await?;

    Ok(())
}

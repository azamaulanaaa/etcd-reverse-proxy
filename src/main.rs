mod election;
mod proxy;

use std::{fs, time::Duration};

use anyhow::Context;
use clap::Parser;
use election::{Election, ElectionConfig, EventType};
use etcd_client::{Client, WatchStream};
use pingora::{listeners::Listeners, server::Server, services::listening::Service};
use proxy::TcpProxyApp;
use serde::Deserialize;
use simple_logger::SimpleLogger;

#[derive(clap::Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long, help = "path of config file")]
    config: String,
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

    let config = {
        let content = fs::read_to_string(args.config)?;
        let config: Config = toml::from_str(&content)?;
        config
    };

    app(config).await?;

    Ok(())
}

#[derive(Deserialize)]
struct Config {
    proxy_name: String,
    advertise_addr: String,
    upstream_addr: String,
    buf_size: usize,

    leader_key: String,
    etcd_addr: String,
}

async fn app(config: Config) -> anyhow::Result<()> {
    let client = Client::connect([config.etcd_addr], None).await?;
    let instance_id = config.advertise_addr.clone();
    let election_config = ElectionConfig {
        leader_key: config.leader_key,
        ttl_second: 60,
        timeout: Duration::from_secs(30),
    };
    let election = Election::new(instance_id.clone(), client, election_config);
    let watch_election_stream = election.watch().await?;

    let proxy_app = {
        let leader_id_option = election.leader_id().await?;
        let init_upstream_addr = if let Some(leader_id) = leader_id_option
            && leader_id != instance_id
        {
            leader_id
        } else {
            config.upstream_addr.clone()
        };

        TcpProxyApp::new(init_upstream_addr, config.buf_size)
    };
    let proxy_service = Service::with_listeners(
        config.proxy_name,
        Listeners::tcp(&config.advertise_addr.clone()),
        proxy_app.clone(),
    );

    let mut server = Server::new(None)?;
    server.bootstrap();
    server.add_service(proxy_service);

    tokio::try_join!(
        async {
            let handle = tokio::spawn(async { server.run_forever() });
            handle.await?;
            Ok(())
        },
        election.run(),
        upstream_updater(
            watch_election_stream,
            &proxy_app,
            instance_id,
            config.upstream_addr.clone()
        )
    )?;

    Ok(())
}

async fn upstream_updater(
    watch_election_stream: WatchStream,
    proxy: &TcpProxyApp,
    instance_id: String,
    upstream_addr: String,
) -> anyhow::Result<()> {
    let mut watch_election_stream = watch_election_stream;
    while let Some(resp) = watch_election_stream
        .message()
        .await
        .context("Watcher stream is failed")?
    {
        for event in resp.events() {
            match event.event_type() {
                EventType::Put => {
                    let leader_id = event
                        .kv()
                        .context("key value not found")?
                        .value_str()
                        .context("value is not valid string")?
                        .to_string();

                    let new_upstream_addr = if leader_id == instance_id {
                        upstream_addr.clone()
                    } else {
                        leader_id
                    };
                    proxy.set_upstream_addr(new_upstream_addr).await;
                }
                _ => continue,
            }
        }
    }

    Ok(())
}

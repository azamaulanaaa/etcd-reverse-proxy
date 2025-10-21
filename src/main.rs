mod election;
mod proxy;

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use election::{Election, ElectionConfig, EventType};
use etcd_client::{Client, WatchStream};
use pingora::{
    listeners::Listeners, server::Server, services::listening::Service, upstreams::peer::BasicPeer,
};
use proxy::TcpProxyApp;
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
        advertise_host: String::from("0.0.0.0"),
        advertise_port: 8080,
        leader_key: String::from("etcd-reverse-proxy/leader"),
        etcd_addr: vec![String::from("node-01-etcd:2379")],
        upstream_host: String::from("216.239.38.120"),
        upstream_port: 443,
        stream_buf_size: 8 * 1024,
    })
    .await?;

    Ok(())
}

struct Config {
    advertise_host: String,
    advertise_port: u16,
    leader_key: String,
    etcd_addr: Vec<String>,
    upstream_host: String,
    upstream_port: u16,
    stream_buf_size: usize,
}

async fn app(config: Config) -> anyhow::Result<()> {
    let upstream_addr = format!("{}:{}", config.upstream_host, config.upstream_port);
    let proxy_app = TcpProxyApp::new(
        BasicPeer::new(&upstream_addr.clone()),
        config.stream_buf_size,
    );

    let listen_addr = format!("{}:{}", config.advertise_host, config.advertise_port);
    let proxy_service = Service::with_listeners(
        String::from("etcd-reverse-proxy"),
        Listeners::tcp(&listen_addr),
        proxy_app.clone(),
    );

    let mut server = Server::new(None)?;
    server.bootstrap();
    server.add_service(proxy_service);

    let client = Client::connect(config.etcd_addr, None).await?;
    let instance_id = format!("{}:{}", config.advertise_host, config.advertise_port);
    let election_config = ElectionConfig {
        leader_key: config.leader_key,
        ttl_second: 60,
        timeout: Duration::from_secs(30),
    };
    let election = Election::new(instance_id.clone(), client, election_config);
    let watch_election_stream = election.watch().await?;

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
            upstream_addr
        )
    )?;

    Ok(())
}

async fn upstream_updater(
    watch_election_stream: WatchStream,
    proxy: &TcpProxyApp<BasicPeer>,
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
                    proxy
                        .set_upstream_peer(BasicPeer::new(&new_upstream_addr))
                        .await;
                }
                _ => continue,
            }
        }
    }

    Ok(())
}

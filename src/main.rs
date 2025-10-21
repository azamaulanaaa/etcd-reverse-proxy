mod election;
mod proxy;

use std::{fs, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use clap::Parser;
use election::{Election, ElectionConfig, EventType};
use etcd_client::Client;
use pingora::{
    listeners::Listeners,
    prelude::background_service,
    server::Server,
    services::{background::BackgroundService, listening::Service},
};
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
        log::LevelFilter::Info
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
    advertise_addr: String,
    upstream_addr: String,
    buf_size: usize,

    leader_key: String,
    etcd_addr: String,
}

async fn app(config: Config) -> anyhow::Result<()> {
    let mut server = Server::new(None)?;
    server.bootstrap();

    let proxy_app = TcpProxyApp::new(config.upstream_addr.clone(), config.buf_size);
    let proxy_service = Service::with_listeners(
        String::from("tcp-proxy"),
        Listeners::tcp(&config.advertise_addr.clone()),
        proxy_app.clone(),
    );
    server.add_service(proxy_service);

    let etcd_client = Client::connect([config.etcd_addr], None).await?;
    let election_app = ElectionApp::new(
        config.advertise_addr,
        config.upstream_addr,
        proxy_app.clone(),
        config.leader_key,
        etcd_client,
    );
    let election_service = background_service("election", election_app);
    server.add_service(election_service);

    server.run_forever();
}

struct ElectionApp {
    instance_id: String,
    upstream_addr: String,
    proxy_app: TcpProxyApp,
    election: Election,
}

impl ElectionApp {
    fn new(
        instance_id: String,
        upstream_addr: String,
        proxy_app: TcpProxyApp,
        leader_key: String,
        etcd_client: Client,
    ) -> Self {
        let election_config = ElectionConfig {
            leader_key: leader_key,
            ttl_second: 60,
            timeout: Duration::from_secs(30),
        };
        let election = Election::new(instance_id.clone(), etcd_client, election_config);

        ElectionApp {
            instance_id,
            upstream_addr,
            proxy_app,
            election,
        }
    }

    async fn watch(&self) -> anyhow::Result<()> {
        let mut watch_stream = self.election.watch().await?;

        while let Some(resp) = watch_stream
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

                        let new_upstream_addr = if leader_id == self.instance_id {
                            self.upstream_addr.clone()
                        } else {
                            leader_id
                        };
                        self.proxy_app.set_upstream_addr(new_upstream_addr).await;
                    }
                    _ => continue,
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl BackgroundService for ElectionApp {
    async fn start(&self, _shutdown: pingora::server::ShutdownWatch) {
        let leader_id = self.election.leader_id().await;
        if let Ok(Some(leader_id)) = leader_id {
            if leader_id != self.instance_id {
                self.proxy_app.set_upstream_addr(leader_id).await;
            }
        }

        let _ = tokio::try_join!(self.election.run(), self.watch());
    }
}

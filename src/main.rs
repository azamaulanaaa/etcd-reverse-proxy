mod election;
mod lua;
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
use tokio::sync::{mpsc, oneshot};

use crate::lua::{LuaApp, LuaHook};

#[derive(clap::Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long, help = "path of config file")]
    config: String,
    #[arg(long, default_value_t = false, help = "Set logger to debug mode")]
    verbose: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;

    let log_level = if args.verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };
    SimpleLogger::new().with_level(log_level).init()?;

    let config_source = fs::read_to_string(args.config)?;

    app(config_source)?;

    Ok(())
}

#[derive(Deserialize, Debug)]
struct Config {
    advertise_addr: String,
    upstream_addr: String,
    buf_size: usize,

    leader_key: String,
    etcd_addr: String,
}

fn app(config_source: String) -> anyhow::Result<()> {
    let config = LuaApp::config::<Config>(config_source.clone())?;

    let mut server = Server::new(None)?;
    server.bootstrap();

    let (lua_app, tx_hook) = LuaApp::new(config_source);
    let lua_service = background_service("lua", lua_app);
    server.add_service(lua_service);

    let proxy_app = TcpProxyApp::new(config.upstream_addr.clone(), config.buf_size);
    let proxy_service = Service::with_listeners(
        String::from("tcp-proxy"),
        Listeners::tcp(&config.advertise_addr.clone()),
        proxy_app.clone(),
    );
    server.add_service(proxy_service);

    let election_app = ElectionApp::new(
        config.advertise_addr,
        config.upstream_addr,
        proxy_app.clone(),
        config.leader_key,
        config.etcd_addr,
        tx_hook,
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
    etcd_addr: String,
    tx_hook: mpsc::Sender<LuaHook>,
}

impl ElectionApp {
    fn new(
        instance_id: String,
        upstream_addr: String,
        proxy_app: TcpProxyApp,
        leader_key: String,
        etcd_addr: String,
        tx_hook: mpsc::Sender<LuaHook>,
    ) -> Self {
        let election_config = ElectionConfig {
            leader_key: leader_key,
            ttl_second: 60,
            timeout: Duration::from_secs(30),
        };
        let election = Election::new(instance_id.clone(), election_config);

        ElectionApp {
            instance_id,
            upstream_addr,
            proxy_app,
            election,
            etcd_addr,
            tx_hook,
        }
    }

    async fn watch(&self, etcd_client: Client) -> anyhow::Result<()> {
        let mut watch_stream = self.election.watch(etcd_client).await?;

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

                        let (tx_res, rx_res) = oneshot::channel();
                        let _ = self
                            .tx_hook
                            .send(LuaHook::OnChange {
                                leader_id: leader_id.clone(),
                                tx_res,
                            })
                            .await?;
                        let _ = rx_res.await?;

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
        let etcd_client = Client::connect([self.etcd_addr.clone()], None)
            .await
            .context("failed to start etcd client")
            .unwrap();

        let leader_id = self.election.leader_id(etcd_client.clone()).await;
        if let Ok(Some(leader_id)) = leader_id {
            if leader_id != self.instance_id {
                self.proxy_app.set_upstream_addr(leader_id).await;
            }
        }

        let _ = tokio::try_join!(
            self.election.run(etcd_client.clone()),
            self.watch(etcd_client.clone())
        )
        .unwrap();
    }
}

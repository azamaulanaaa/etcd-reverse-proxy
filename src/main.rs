mod election;
mod lua;
mod proxy;

use crate::{
    election::{ElectionApp, ElectionEvent},
    lua::{LuaApp, LuaHook},
    proxy::TcpProxyApp,
};
use std::fs;

use anyhow::Context;
use async_trait::async_trait;
use clap::Parser;
use pingora::{
    listeners::Listeners,
    prelude::background_service,
    server::Server,
    services::{background::BackgroundService, listening::Service},
};
use serde::Deserialize;
use simple_logger::SimpleLogger;
use tokio::sync::{Mutex, mpsc, oneshot};

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

    let (election_app, rx_election_event) = ElectionApp::new(
        config.advertise_addr.clone(),
        config.leader_key,
        config.etcd_addr,
    );
    let election_service = background_service("election", election_app);
    server.add_service(election_service);

    let bridge = Bridge {
        instance_id: config.advertise_addr,
        upstream_addr: config.upstream_addr,
        tx_lua_hook: tx_hook,
        proxy_app,
        rx_election_event: Mutex::new(rx_election_event),
    };
    let bridge_service = background_service("brider", bridge);
    server.add_service(bridge_service);

    server.run_forever();
}

struct Bridge {
    instance_id: String,
    upstream_addr: String,
    proxy_app: TcpProxyApp,
    tx_lua_hook: mpsc::Sender<LuaHook>,
    rx_election_event: Mutex<mpsc::Receiver<ElectionEvent>>,
}

impl Bridge {
    async fn call_on_change(&self, leader_id: Option<String>) -> anyhow::Result<()> {
        let (tx_res, rx_res) = oneshot::channel();
        let _ = self
            .tx_lua_hook
            .send(LuaHook::OnChange {
                leader_id: leader_id.unwrap(),
                tx_res,
            })
            .await
            .context("Failed to call hook's on change")?;
        let _ = rx_res
            .await
            .context("Failed to receive hook's on change response")?;

        Ok(())
    }

    async fn update_upstream_addr(&self, leader_id: Option<String>) -> anyhow::Result<()> {
        let leader_id = match leader_id {
            Some(v) => v,
            None => return Ok(()),
        };

        let new_upstream_addr = if self.instance_id == leader_id {
            self.upstream_addr.clone()
        } else {
            leader_id
        };
        self.proxy_app.set_upstream_addr(new_upstream_addr).await;

        Ok(())
    }
}

#[async_trait]
impl BackgroundService for Bridge {
    async fn start(&self, _shutdown: pingora::server::ShutdownWatch) {
        let mut rx_election_event = self.rx_election_event.lock().await;

        while let Some(election_event) = rx_election_event.recv().await {
            match election_event {
                ElectionEvent::LeaderChanged { leader_id } => {
                    if let Err(e) = self.update_upstream_addr(leader_id.clone()).await {
                        log::error!("{}", e);
                    }
                    if let Err(e) = self.call_on_change(leader_id).await {
                        log::error!("{}", e);
                    }
                }
            }
        }
    }
}

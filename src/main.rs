mod election;
mod lua;
mod proxy;

use crate::{
    election::{ElectionApp, ElectionEvent},
    lua::LuaE,
    proxy::{TcpProxyApp, TcpProxyConfig},
};
use std::{fmt::Debug, path::Path};

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
use tokio::sync::{Mutex, mpsc};

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

    app(args.config)?;

    Ok(())
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    advertise_addr: String,
    upstream_addr: String,
    buf_size: usize,

    leader_key: String,
    etcd_addr: String,
}

fn app(config_path: String) -> anyhow::Result<()> {
    let config_path = Path::new(&config_path);

    let luae = LuaE::init(&config_path)?;
    let config = luae.config::<Config>()?;

    let mut server = Server::new(None)?;
    server.bootstrap();

    let (proxy_app, rx_proxy_config) = TcpProxyApp::new(config.buf_size);
    let proxy_service = Service::with_listeners(
        String::from("tcp-proxy"),
        Listeners::tcp(&config.advertise_addr.clone()),
        proxy_app,
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
        luae,
        state: Mutex::new(BridgeState {
            upstream_addr: None,
        }),
        instance_id: config.advertise_addr,
        local_upstream_addr: config.upstream_addr,
        rx_proxy_config: Mutex::new(rx_proxy_config),
        rx_election_event: Mutex::new(rx_election_event),
    };
    let bridge_service = background_service("brider", bridge);
    server.add_service(bridge_service);

    server.run_forever();
}

struct BridgeState {
    upstream_addr: Option<String>,
}

struct Bridge {
    luae: LuaE,
    state: Mutex<BridgeState>,
    instance_id: String,
    local_upstream_addr: String,
    rx_proxy_config: Mutex<mpsc::Receiver<TcpProxyConfig>>,
    rx_election_event: Mutex<mpsc::Receiver<ElectionEvent>>,
}
#[async_trait]
impl BackgroundService for Bridge {
    async fn start(&self, _shutdown: pingora::server::ShutdownWatch) {
        let mut rx_proxy_config = self.rx_proxy_config.lock().await;
        let mut rx_election_event = self.rx_election_event.lock().await;

        loop {
            tokio::select! {
                Some(proxy_config) = rx_proxy_config.recv() => {
                    match proxy_config {
                        TcpProxyConfig::GetUpstreamAddr{ tx_res } => {
                            let state = self.state.lock().await;
                            if let Err(_e) = tx_res.send(Ok(state.upstream_addr.clone())) {
                                log::error!("Failed to send tcp proxy config's get upstream addr response");
                            }
                        }
                    }
                }
                Some(election_event) = rx_election_event.recv() => {
                    match election_event {
                        ElectionEvent::LeaderChanged { leader_id } => {
                            {
                                let new_upstream_addr = if Some(self.instance_id.clone()) == leader_id {
                                    Some(self.local_upstream_addr.clone())
                                } else {
                                    leader_id.clone()
                                };

                                let mut state = self.state.lock().await;
                                state.upstream_addr = new_upstream_addr;
                            }

                            if let Err(e) = self.luae.hook().on_change(leader_id) {
                                log::error!("{}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}

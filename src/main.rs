mod election;
mod lua;
mod proxy;

use crate::{election::ElectionApp, lua::LuaApp, proxy::TcpProxyApp};
use std::fs;

use clap::Parser;
use pingora::{
    listeners::Listeners, prelude::background_service, server::Server, services::listening::Service,
};
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

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pingora::{
    connectors::TransportConnector, protocols::Stream, server::ShutdownWatch,
    upstreams::peer::BasicPeer,
};
use tokio::{
    io::copy_bidirectional_with_sizes,
    net::lookup_host,
    sync::{mpsc, oneshot},
};

pub enum TcpProxyConfig {
    GetUpstreamAddr {
        tx_res: oneshot::Sender<anyhow::Result<Option<String>>>,
    },
}

pub struct TcpProxyApp {
    client_connector: TransportConnector,
    buf_size: usize,
    tx_config: mpsc::Sender<TcpProxyConfig>,
}

impl TcpProxyApp {
    pub fn new(buf_size: usize) -> (Self, mpsc::Receiver<TcpProxyConfig>) {
        let (tx_config, rx_config) = mpsc::channel::<TcpProxyConfig>(32);

        (
            Self {
                buf_size,
                client_connector: TransportConnector::new(None),
                tx_config,
            },
            rx_config,
        )
    }

    async fn gen_peer(&self, upstream_addr: String) -> anyhow::Result<BasicPeer> {
        let mut addrs = lookup_host(upstream_addr.clone()).await?;
        let first_addr = addrs.nth(0).ok_or(anyhow::anyhow!(
            "Failed to resolve '{}'",
            upstream_addr.clone()
        ))?;
        let peer = BasicPeer::new(&first_addr.to_string());
        Ok(peer)
    }
}

#[async_trait]
impl pingora::apps::ServerApp for TcpProxyApp {
    async fn process_new(
        self: &Arc<Self>,
        mut server_stream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<pingora::protocols::Stream> {
        log::debug!("Get lastest upstream address");
        let upstream_addr = {
            let (tx_res, rx_res) = oneshot::channel();
            if let Err(e) = self
                .tx_config
                .send(TcpProxyConfig::GetUpstreamAddr { tx_res })
                .await
                .context("Failed to call get upstream addr")
            {
                log::error!("{}", e);
                return None;
            }
            let upstream_addr = rx_res
                .await
                .context("Failed to receive get upstream addr")
                .flatten();
            let upstream_addr = match upstream_addr {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{}", e);
                    return None;
                }
            };

            upstream_addr
        };
        let upstream_addr = match upstream_addr {
            Some(v) => v,
            None => {
                log::error!("Upstream address is empty");
                return None;
            }
        };
        log::debug!("Upstream address : {}", upstream_addr);

        log::debug!("Generate an upstream peer");
        let peer = match self
            .gen_peer(upstream_addr.clone())
            .await
            .context("Failed create upstream peer")
        {
            Ok(peer) => peer,
            Err(e) => {
                log::error!("{}", e);
                return None;
            }
        };

        log::debug!("Start streaming to upstream peer");
        let client_stream = self
            .client_connector
            .new_stream(&peer)
            .await
            .context("Create client stream is failed");
        let mut client_stream = match client_stream {
            Ok(client_stream) => client_stream,
            Err(e) => {
                log::error!("{}", e);
                return None;
            }
        };
        if let Err(e) = copy_bidirectional_with_sizes(
            &mut server_stream,
            &mut client_stream,
            self.buf_size,
            self.buf_size,
        )
        .await
        .context("Duplex stream is crash")
        {
            log::error!("{}", e);
        }

        return None;
    }
}

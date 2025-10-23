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
            "no ip address exist for '{}'",
            upstream_addr.clone()
        ))?;
        let peer = BasicPeer::new(&first_addr.to_string());
        Ok(peer)
    }

    async fn duplex(
        &self,
        mut server_stream: Stream,
        mut client_stream: Stream,
        buf_size: usize,
    ) -> anyhow::Result<()> {
        tokio::select! {
            res = copy_bidirectional_with_sizes(&mut server_stream, &mut client_stream, buf_size, buf_size) => {
                res.context("Stream duplexing failed")?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl pingora::apps::ServerApp for TcpProxyApp {
    async fn process_new(
        self: &Arc<Self>,
        server_stream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<pingora::protocols::Stream> {
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

        let peer = match self.gen_peer(upstream_addr.clone()).await {
            Ok(peer) => peer,
            Err(e) => {
                log::error!("unable to create peer: {:?}", e);
                return None;
            }
        };

        let client_stream = self.client_connector.new_stream(&peer).await;

        match client_stream {
            Ok(client_stream) => {
                if let Err(e) = self
                    .duplex(server_stream, client_stream, self.buf_size)
                    .await
                {
                    log::error!("duplex stream failed: {:?}", e);
                }
            }
            Err(e) => {
                log::error!("create client stream is failed: {:?}", e)
            }
        }

        return None;
    }
}

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pingora::{
    connectors::TransportConnector, protocols::Stream, server::ShutdownWatch,
    upstreams::peer::BasicPeer,
};
use tokio::{io::copy_bidirectional_with_sizes, net::lookup_host, sync::Mutex};

pub struct TcpProxyApp {
    upstream_addr: Arc<Mutex<String>>,
    client_connector: TransportConnector,
    buf_size: usize,
}

impl TcpProxyApp {
    pub fn new(upstream_addr: String, buf_size: usize) -> Self {
        Self {
            upstream_addr: Arc::new(Mutex::new(upstream_addr)),
            buf_size,
            client_connector: TransportConnector::new(None),
        }
    }

    pub async fn set_upstream_addr(&self, new_upstream_addr: String) {
        let mut upstream_addr = self.upstream_addr.lock().await;
        *upstream_addr = new_upstream_addr;
        log::info!("Upstream addr updated");
    }

    async fn watch_upstream_state_changes(&self, upstream_addr: String) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        loop {
            interval.tick().await;
            let current_upstream_addr = self.upstream_addr.lock().await;
            if *current_upstream_addr != upstream_addr {
                break;
            }
        }
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
        upstream_addr: String,
    ) -> anyhow::Result<()> {
        tokio::select! {
            res = copy_bidirectional_with_sizes(&mut server_stream, &mut client_stream, buf_size, buf_size) => {
                res.context("Stream duplexing failed")?;
            }
            _ = self.watch_upstream_state_changes(upstream_addr) => {
                log::info!("Upstream peer changed");
                return Err(anyhow::anyhow!("Upstream peer configuration changed"));
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
        let upstream_addr = { self.upstream_addr.lock().await.clone() };
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
                    .duplex(
                        server_stream,
                        client_stream,
                        self.buf_size,
                        upstream_addr.clone(),
                    )
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

impl Clone for TcpProxyApp {
    fn clone(&self) -> Self {
        TcpProxyApp {
            upstream_addr: self.upstream_addr.clone(),
            client_connector: TransportConnector::new(None),
            buf_size: self.buf_size,
        }
    }
}

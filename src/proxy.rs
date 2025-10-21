use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pingora::{
    connectors::TransportConnector, protocols::Stream, server::ShutdownWatch, upstreams::peer::Peer,
};
use tokio::{io::copy_bidirectional_with_sizes, sync::Mutex};

pub struct TcpProxyApp<P>
where
    P: Peer + Send + Sync + 'static,
{
    upstream_state: Arc<Mutex<PeerState<P>>>,
    client_connector: TransportConnector,
    buf_size: usize,
}

impl<P> TcpProxyApp<P>
where
    P: Peer + Send + Sync + 'static,
{
    pub fn new(upstream_peer: P, buf_size: usize) -> Self {
        Self {
            upstream_state: Arc::new(Mutex::new(PeerState {
                peer: upstream_peer,
                id: 0,
            })),
            buf_size,
            client_connector: TransportConnector::new(None),
        }
    }

    pub async fn set_upstream_peer(&mut self, new_upstream_peer: P) {
        let mut upstream_state = self.upstream_state.lock().await;
        upstream_state.peer = new_upstream_peer;
        upstream_state.id = upstream_state.id.checked_add(1).unwrap_or(0);
        log::info!("Upstream peer updated");
    }

    async fn watch_upstream_state_changes(&self, initial_peer_id: usize) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        loop {
            interval.tick().await;
            let current_id = self.upstream_state.lock().await.id;
            if current_id != initial_peer_id {
                break;
            }
        }
    }

    async fn duplex(
        &self,
        mut server_stream: Stream,
        mut client_stream: Stream,
        buf_size: usize,
        peer_id: usize,
    ) -> anyhow::Result<()> {
        tokio::select! {
            res = copy_bidirectional_with_sizes(&mut server_stream, &mut client_stream, buf_size, buf_size) => {
                res.context("Stream duplexing failed")?;
            }
            _ = self.watch_upstream_state_changes(peer_id) => {
                log::info!("Upstream peer changed");
                return Err(anyhow::anyhow!("Upstream peer configuration changed"));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<P> pingora::apps::ServerApp for TcpProxyApp<P>
where
    P: Peer + Send + Sync + 'static,
{
    async fn process_new(
        self: &Arc<Self>,
        server_stream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<pingora::protocols::Stream> {
        let (peer, peer_id) = {
            let peer_state = self.upstream_state.lock().await;
            let peer = peer_state.peer.clone();
            let peer_id = peer_state.id;
            (peer, peer_id)
        };

        let client_stream = self.client_connector.new_stream(&peer).await;

        match client_stream {
            Ok(client_stream) => {
                if let Err(e) = self
                    .duplex(server_stream, client_stream, self.buf_size, peer_id)
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

pub struct PeerState<P>
where
    P: Peer + Send + Sync,
{
    pub peer: P,
    pub id: usize,
}

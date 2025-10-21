use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use pingora::{
    connectors::TransportConnector, protocols::Stream, server::ShutdownWatch, upstreams::peer::Peer,
};
use tokio::io::copy_bidirectional_with_sizes;

pub struct TcpProxy<P>
where
    P: Peer + Send + Sync + 'static,
{
    upstream_peer: P,
    client_connector: TransportConnector,
    buf_size: usize,
}

impl<P> TcpProxy<P>
where
    P: Peer + Send + Sync + 'static,
{
    pub fn new(upstream_peer: P, buf_size: usize) -> Self {
        Self {
            upstream_peer,
            buf_size,
            client_connector: TransportConnector::new(None),
        }
    }

    async fn duplex(
        &self,
        mut server_stream: Stream,
        mut client_stream: Stream,
        buf_size: usize,
    ) -> anyhow::Result<()> {
        copy_bidirectional_with_sizes(&mut server_stream, &mut client_stream, buf_size, buf_size)
            .await
            .context("Stream duplexing failed")?;

        Ok(())
    }
}

#[async_trait]
impl<P> pingora::apps::ServerApp for TcpProxy<P>
where
    P: Peer + Send + Sync + 'static,
{
    async fn process_new(
        self: &Arc<Self>,
        server_stream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<pingora::protocols::Stream> {
        let client_stream = self.client_connector.new_stream(&self.upstream_peer).await;

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

use tokio::time::{Duration, sleep};

use anyhow::Context;
use etcd_client::{Client, Compare, CompareOp, PutOptions, Txn, TxnOp};
pub use etcd_client::{Event, EventType, WatchResponse, WatchStream};

pub struct ElectionConfig {
    pub leader_key: String,
    pub ttl_second: u16,
    pub timeout: Duration,
}

pub struct Election {
    instance_id: String,
    client: Client,
    config: ElectionConfig,
    retry_delay: Duration,
}

impl Election {
    pub fn new(instance_id: String, client: Client, config: ElectionConfig) -> Self {
        Self {
            instance_id,
            client,
            config,
            retry_delay: Duration::from_millis(rand::random_range(60..=60 * 1000)),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        loop {
            log::debug!("Request for lease id");
            let lease_id = match self.create_lease().await {
                Ok(id) => id,
                Err(e) => {
                    log::error!("Failed to create lease: {:?}", e);
                    sleep(self.config.timeout).await;
                    continue;
                }
            };
            log::debug!("Granted lease id: {:?}", lease_id);

            log::debug!("Start campaign");
            match self.try_campaign(lease_id).await {
                Ok(true) => {
                    log::info!("Status: client is a leader");
                    if let Err(e) = self.maintain_leadership(lease_id).await {
                        log::warn!("Leader maintenance failed: {:?}", e);
                    }
                }
                Ok(false) => {
                    log::info!("Status: client is a follower");
                }
                Err(e) => {
                    log::error!("Transaction failed: {:?}", e);
                }
            }

            let _ = self.client.lease_client().revoke(lease_id).await;
            sleep(self.retry_delay).await;
        }
    }

    async fn create_lease(&self) -> anyhow::Result<i64> {
        let resp = self
            .client
            .lease_client()
            .grant(self.config.ttl_second as i64, None)
            .await
            .context("Lease grant failed")?;
        Ok(resp.id())
    }

    async fn try_campaign(&self, lease_id: i64) -> anyhow::Result<bool> {
        let compare = Compare::version(self.config.leader_key.clone(), CompareOp::Equal, 0);

        let put_options = PutOptions::new().with_lease(lease_id);
        let put_op = TxnOp::put(
            self.config.leader_key.clone(),
            self.instance_id.clone(),
            Some(put_options),
        );

        let get_op = TxnOp::get(self.config.leader_key.clone(), None);

        let txn = Txn::new()
            .when(vec![compare])
            .and_then(vec![put_op])
            .or_else(vec![get_op]);

        let resp = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .context("Transaction failed")?;

        Ok(resp.succeeded())
    }

    async fn maintain_leadership(&self, lease_id: i64) -> anyhow::Result<()> {
        let keepalive_timeout = Duration::from_secs(self.config.ttl_second as u64 / 2);
        let mut lease_client = self.client.lease_client();

        log::debug!("Start KeepAlive");
        let (mut keeper, mut response_stream) = lease_client
            .keep_alive(lease_id)
            .await
            .context("Failed to start KeepAlive")?;
        log::debug!("KeepAlive started");

        loop {
            log::debug!("Send KeepAlive message");
            keeper
                .keep_alive()
                .await
                .context("Send KeepAlive request")?;

            tokio::select! {
                renewal = response_stream.message() => {
                    match renewal {
                        Ok(Some(_)) => {
                            log::debug!("KeepAlive's response recieved");
                            sleep(keepalive_timeout).await;
                        }
                        Ok(None) => {
                            return Err(anyhow::anyhow!("KeepAlive stream closed by etcd (possible loss of leadership)" ));
                        }
                        Err(e) => {
                            return Err(anyhow::Error::new(e).context("KeepAlive stream error"));
                        }
                    }
                }
                _ = sleep(keepalive_timeout) => {
                    return Err(anyhow::anyhow!("Timeout waiting for KeepAlive responses"));
                }

            }
        }
    }

    pub async fn watch(&self) -> anyhow::Result<WatchStream> {
        let mut watcher_client = self.client.watch_client().clone();

        log::debug!("Watching leader value changes");
        let (_, stream) = watcher_client
            .watch(self.config.leader_key.clone(), None)
            .await
            .context("Failed to start watch")?;

        return Ok(stream);
    }
}

use crate::lua::LuaHook;
use crate::proxy::TcpProxyApp;

use std::ops::Add;

use anyhow::Context;
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, EventType, PutOptions, Txn, TxnOp, WatchStream};
use pingora::services::background::BackgroundService;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, sleep},
};

pub struct ElectionConfig {
    pub leader_key: String,
    pub ttl_second: Duration,
    pub timeout: Duration,
}

pub struct Election {
    instance_id: String,
    config: ElectionConfig,
    retry_delay: Duration,
}

impl Election {
    pub fn new(instance_id: String, config: ElectionConfig) -> Self {
        Self {
            instance_id,
            config,
            retry_delay: Duration::from_millis(rand::random_range(1..=5 * 1000)),
        }
    }

    pub async fn run(&self, etcd_client: Client) -> anyhow::Result<()> {
        loop {
            log::debug!("Request for lease id");
            let lease_id = match self.create_lease(etcd_client.clone()).await {
                Ok(id) => id,
                Err(e) => {
                    log::error!("Failed to create lease: {:?}", e);
                    sleep(self.config.timeout).await;
                    continue;
                }
            };
            log::debug!("Granted lease id: {:?}", lease_id);

            log::debug!("Start campaign");
            match self.try_campaign(lease_id, etcd_client.clone()).await {
                Ok(true) => {
                    log::info!("Status: client is a leader");
                    if let Err(e) = self
                        .maintain_leadership(lease_id, etcd_client.clone())
                        .await
                    {
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

            let _ = etcd_client.lease_client().revoke(lease_id).await;
            sleep(self.retry_delay.add(self.config.ttl_second)).await;
        }
    }

    async fn create_lease(&self, etcd_client: Client) -> anyhow::Result<i64> {
        let lease_time: i64 = self
            .config
            .ttl_second
            .as_secs()
            .try_into()
            .context("time to live is not a valid i64 value")?;
        let resp = etcd_client
            .lease_client()
            .grant(lease_time, None)
            .await
            .context("Lease grant failed")?;
        Ok(resp.id())
    }

    async fn try_campaign(&self, lease_id: i64, etcd_client: Client) -> anyhow::Result<bool> {
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

        let resp = etcd_client
            .kv_client()
            .txn(txn)
            .await
            .context("Transaction failed")?;

        Ok(resp.succeeded())
    }

    async fn maintain_leadership(&self, lease_id: i64, etcd_client: Client) -> anyhow::Result<()> {
        let keepalive_timeout = self.config.ttl_second.div_f64(2.0);
        let mut lease_client = etcd_client.lease_client();

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

    pub async fn watch(&self, etcd_client: Client) -> anyhow::Result<WatchStream> {
        let mut watcher_client = etcd_client.watch_client().clone();

        log::debug!("Watching leader value changes");
        let (_, stream) = watcher_client
            .watch(self.config.leader_key.clone(), None)
            .await
            .context("Failed to start watch")?;

        return Ok(stream);
    }

    pub async fn leader_id(&self, etcd_client: Client) -> anyhow::Result<Option<String>> {
        let mut kv_client = etcd_client.kv_client().clone();

        let resp = kv_client
            .get(self.config.leader_key.clone(), None)
            .await
            .context("Failed to get leader id")?;
        let value = resp
            .kvs()
            .get(0)
            .map(|kv| kv.value_str())
            .transpose()?
            .map(|s| s.to_owned());

        Ok(value)
    }
}

pub struct ElectionApp {
    instance_id: String,
    upstream_addr: String,
    proxy_app: TcpProxyApp,
    election: Election,
    etcd_addr: String,
    tx_hook: mpsc::Sender<LuaHook>,
}

impl ElectionApp {
    pub fn new(
        instance_id: String,
        upstream_addr: String,
        proxy_app: TcpProxyApp,
        leader_key: String,
        etcd_addr: String,
        tx_hook: mpsc::Sender<LuaHook>,
    ) -> Self {
        let election_config = ElectionConfig {
            leader_key: leader_key,
            ttl_second: Duration::from_secs(60),
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

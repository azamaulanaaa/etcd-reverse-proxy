use anyhow::Context;
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, EventType, PutOptions, Txn, TxnOp, WatchStream};
use pingora::services::background::BackgroundService;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};

pub enum ElectionEvent {
    LeaderChanged { leader_id: Option<String> },
}

pub struct ElectionConfig {
    pub leader_key: String,
    pub heartbeat_interval: Duration,
}

pub struct Election {
    instance_id: String,
    config: ElectionConfig,
    tx_event: mpsc::Sender<ElectionEvent>,
}

impl Election {
    pub fn new(
        instance_id: String,
        config: ElectionConfig,
    ) -> (Self, mpsc::Receiver<ElectionEvent>) {
        let (tx_event, rx_event) = mpsc::channel::<ElectionEvent>(32);

        (
            Self {
                instance_id,
                config,
                tx_event,
            },
            rx_event,
        )
    }

    pub async fn run(&self, etcd_client: Client) -> anyhow::Result<()> {
        log::debug!("Start watcher");
        let mut watch_stream = self
            .watch(etcd_client.clone())
            .await
            .context("Failed to create watcher's stream")?;

        let leader_id = self
            .leader_id(etcd_client.clone())
            .await
            .context("Failed to get initial leader id")?;
        self.tx_event
            .send(ElectionEvent::LeaderChanged {
                leader_id: leader_id.clone(),
            })
            .await
            .context("Unable to send an election event")?;

        self.try_campaign(etcd_client.clone()).await?;
        if Some(self.instance_id.clone()) == leader_id {
            log::info!("Client is a LEADER");
        }

        while let Some(resp) = watch_stream
            .message()
            .await
            .context("Failed to receive watcher's message")?
        {
            for event in resp.events() {
                if event.event_type() == EventType::Delete {
                    self.try_campaign(etcd_client.clone()).await?;
                }

                let leader_id = event
                    .kv()
                    .map(|kv| kv.value_str())
                    .transpose()
                    .context("Value is not valid string")?
                    .map(|v| v.to_string());

                self.tx_event
                    .send(ElectionEvent::LeaderChanged { leader_id })
                    .await
                    .context("Unable to send an election event")?;
            }
        }

        Ok(())
    }

    async fn try_campaign(&self, etcd_client: Client) -> anyhow::Result<()> {
        log::debug!("Request for a lease id");
        let lease_id = self.request_lease_id(etcd_client.clone()).await?;
        log::debug!("Lease id: {}", lease_id);

        log::debug!("Start a campaign");
        match self.ask_for_leadership(lease_id, etcd_client.clone()).await {
            Ok(true) => {
                log::info!("Client is a LEADER");
                if let Err(e) = self
                    .maintain_leadership(lease_id, etcd_client.clone())
                    .await
                {
                    log::warn!("Leader maintenance failed: {}", e);
                }
            }
            Ok(false) => {
                log::info!("Client is a FOLLOWER");
            }
            Err(e) => {
                log::error!("Campaign failed: {}", e);
            }
        }

        let _ = etcd_client.lease_client().revoke(lease_id).await;

        Ok(())
    }

    async fn request_lease_id(&self, etcd_client: Client) -> anyhow::Result<i64> {
        let lease_time: i64 = self
            .config
            .heartbeat_interval
            .as_secs()
            .try_into()
            .context("Heartbeat interval as seconds is not valid i64")?;
        let resp = etcd_client
            .lease_client()
            .grant(lease_time, None)
            .await
            .context("Request lease failed")?;
        Ok(resp.id())
    }

    async fn ask_for_leadership(&self, lease_id: i64, etcd_client: Client) -> anyhow::Result<bool> {
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
            .context("Campaign transaction failed")?;

        Ok(resp.succeeded())
    }

    async fn maintain_leadership(&self, lease_id: i64, etcd_client: Client) -> anyhow::Result<()> {
        let mut lease_client = etcd_client.lease_client();

        log::debug!("Start KeepAlive");
        let (mut keeper, mut response_stream) = lease_client
            .keep_alive(lease_id)
            .await
            .context("Failed to start KeepAlive")?;

        loop {
            log::debug!("Send KeepAlive message");
            keeper
                .keep_alive()
                .await
                .context("Unable to send KeepAlive message")?;

            tokio::select! {
                renewal = response_stream.message() => {
                    match renewal {
                        Ok(Some(_)) => {
                            log::debug!("KeepAlive's response recieved");
                            sleep(self.config.heartbeat_interval.div_f64(2.0)).await;
                        }
                        Ok(None) => {
                            return Err(anyhow::anyhow!("KeepAlive stream closed by etcd (possible loss of leadership)" ));
                        }
                        Err(e) => {
                            return Err(anyhow::Error::new(e).context("Unable to recieve KeepAlive response"));
                        }
                    }
                }
                _ = sleep(self.config.heartbeat_interval) => {
                    return Err(anyhow::anyhow!("Timeout waiting for KeepAlive responses"));
                }

            }
        }
    }

    async fn watch(&self, etcd_client: Client) -> anyhow::Result<WatchStream> {
        let mut watcher_client = etcd_client.watch_client().clone();

        let (_, watcher_stream) = watcher_client
            .watch(self.config.leader_key.clone(), None)
            .await
            .context("Failed to start watcher")?;

        Ok(watcher_stream)
    }

    pub async fn leader_id(&self, etcd_client: Client) -> anyhow::Result<Option<String>> {
        let mut kv_client = etcd_client.kv_client().clone();

        let resp = kv_client
            .get(self.config.leader_key.clone(), None)
            .await
            .context("Failed to get leader id")?;
        let value = resp
            .kvs().first()
            .map(|kv| kv.value_str())
            .transpose()?
            .map(|s| s.to_owned());

        Ok(value)
    }
}

pub struct ElectionApp {
    election: Election,
    etcd_addr: String,
}

impl ElectionApp {
    pub fn new(
        instance_id: String,
        leader_key: String,
        etcd_addr: String,
    ) -> (Self, mpsc::Receiver<ElectionEvent>) {
        let election_config = ElectionConfig {
            leader_key,
            heartbeat_interval: Duration::from_secs(60),
        };
        let (election, rx_event) = Election::new(instance_id.clone(), election_config);

        (
            ElectionApp {
                election,
                etcd_addr,
            },
            rx_event,
        )
    }
}

#[async_trait]
impl BackgroundService for ElectionApp {
    async fn start(&self, _shutdown: pingora::server::ShutdownWatch) {
        log::debug!("Crate an etcd client");
        let etcd_client = Client::connect([self.etcd_addr.clone()], None)
            .await
            .context("Failed to start etcd client");
        let etcd_client = match etcd_client {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };

        log::debug!("Start election");
        let result = self
            .election
            .run(etcd_client.clone())
            .await
            .context("Election is crash");

        let _result = match result {
            Ok(v) => v,
            Err(e) => {
                log::error!("{}", e);
                return;
            }
        };
    }
}

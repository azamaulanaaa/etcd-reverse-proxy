use std::fmt::Debug;

use async_trait::async_trait;
use mlua::{Lua, LuaSerdeExt};
use pingora::services::background::BackgroundService;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, mpsc, oneshot};

pub enum LuaHook {
    OnChange {
        leader_id: String,
        tx_res: oneshot::Sender<mlua::Result<()>>,
    },
}

pub struct LuaApp {
    source: String,
    rx_req: Mutex<mpsc::Receiver<LuaHook>>,
}

impl LuaApp {
    pub fn new(source: String) -> (Self, mpsc::Sender<LuaHook>) {
        let (tx_req, rx_req) = mpsc::channel::<LuaHook>(32);
        let rx_req = Mutex::new(rx_req);

        (Self { source, rx_req }, tx_req)
    }

    pub fn config<C>(source: String) -> anyhow::Result<C>
    where
        C: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        let lua = Lua::new();
        let lua_main_table: mlua::Table = lua.load(source).eval()?;
        let config: C = lua.from_value(lua_main_table.get("config")?)?;

        Ok(config)
    }

    fn run(req: LuaHook, source: String) -> anyhow::Result<()> {
        let lua = Lua::new();
        let lua_main_table: mlua::Table = lua.load(source).eval()?;
        let hook: Option<mlua::Table> = lua_main_table.get("hook")?;
        let hook = match hook {
            Some(v) => v,
            None => return Ok(()),
        };
        let on_change: Option<mlua::Function> = hook.get("on_change")?;

        match req {
            LuaHook::OnChange {
                leader_id,
                tx_res: tx,
            } => {
                if let Some(on_change) = on_change {
                    let args = lua.create_table()?;
                    args.set("leader_id", leader_id)?;

                    let out = on_change.call::<()>(args);
                    let _ = tx.send(out);
                }
            }
        };

        Ok(())
    }
}

#[async_trait]
impl BackgroundService for LuaApp {
    async fn start(&self, mut _shutdown: pingora::server::ShutdownWatch) {
        let mut rx_req = self.rx_req.lock().await;

        while let Some(req) = rx_req.recv().await {
            let source = self.source.clone();
            tokio::task::spawn_blocking(|| Self::run(req, source).unwrap());
        }
    }
}

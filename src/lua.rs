use std::fmt::Debug;

use anyhow::Context;
use async_trait::async_trait;
use mlua::{Lua, LuaSerdeExt};
use pingora::services::background::BackgroundService;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, mpsc, oneshot};

pub enum LuaCommand {
    OnChange {
        leader_id: String,
        tx_res: oneshot::Sender<mlua::Result<()>>,
    },
}

pub struct LuaApp {
    source: String,
    rx_command: Mutex<mpsc::Receiver<LuaCommand>>,
}

impl LuaApp {
    pub fn new(source: String) -> (Self, mpsc::Sender<LuaCommand>) {
        let (tx_command, rx_command) = mpsc::channel::<LuaCommand>(32);
        let rx_command = Mutex::new(rx_command);

        (Self { source, rx_command }, tx_command)
    }

    pub fn config<C>(source: String) -> anyhow::Result<C>
    where
        C: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        let lua = Lua::new();

        log::debug!("Evaluate lua script");
        let lua_main_table: mlua::Table = lua
            .load(source)
            .eval()
            .context("Failed to evaluate lua script")?;

        log::debug!("Parse lua config");
        let config: C = lua.from_value(
            lua_main_table
                .get("config")
                .context("Config's field is malformed")?,
        )?;

        Ok(config)
    }

    fn run(req: LuaCommand, source: String) -> anyhow::Result<()> {
        let lua = Lua::new();
        log::debug!("Evaluate lua script");
        let lua_main_table: mlua::Table = lua
            .load(source)
            .eval()
            .context("Failed to evaluate lua script")?;

        log::debug!("Parse lua hook");
        let hook: Option<mlua::Table> = lua_main_table
            .get("hook")
            .context("Hook's field is malformed")?;
        let hook = match hook {
            Some(v) => v,
            None => return Ok(()),
        };

        log::debug!("Parse on change hook");
        let on_change: Option<mlua::Function> = hook
            .get("on_change")
            .context("Hook on change is malformed")?;

        match req {
            LuaCommand::OnChange {
                leader_id,
                tx_res: tx,
            } => {
                log::debug!("Call lua command on change");
                if let Some(on_change) = on_change {
                    let args = lua
                        .create_table()
                        .context("Failed to create 'args' as lua table for on change hooks")?;
                    args.set("leader_id", leader_id)
                        .context("Failed to set args 'leader_id'")?;

                    let out = on_change.call::<()>(args);
                    let _ = tx
                        .send(out)
                        .map_err(|_| anyhow::anyhow!("Failed to call on change hook"))?;
                }
            }
        };

        Ok(())
    }
}

#[async_trait]
impl BackgroundService for LuaApp {
    async fn start(&self, mut _shutdown: pingora::server::ShutdownWatch) {
        let mut rx_command = self.rx_command.lock().await;

        while let Some(command) = rx_command.recv().await {
            let source = self.source.clone();
            tokio::task::spawn_blocking(|| Self::run(command, source).unwrap());
        }
    }
}

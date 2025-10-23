use std::fmt::Debug;

use anyhow::Context;
use mlua::{FromLua, Lua, LuaSerdeExt};
use serde::de::DeserializeOwned;

pub struct LuaHook {
    lua: Lua,
    on_change: Option<mlua::Function>,
}

impl LuaHook {
    pub fn on_change(&self, leader_id: Option<String>) -> anyhow::Result<()> {
        if let Some(on_change) = &self.on_change {
            let args = &self
                .lua
                .create_table()
                .context("Failed to create 'args' as lua table for on change hooks")?;
            args.set("leader_id", leader_id)
                .context("Failed to set args 'leader_id'")?;

            let out = on_change.call::<()>(args)?;

            return Ok(out);
        }

        Ok(())
    }
}

impl FromLua for LuaHook {
    fn from_lua(value: mlua::Value, lua: &Lua) -> mlua::Result<Self> {
        let hooks_table = Option::<mlua::Table>::from_lua(value, lua)
            .context("Expectiong hooks to be a table")?;

        Ok(Self {
            lua: lua.clone(),
            on_change: hooks_table
                .map(|hooks_table| hooks_table.get("on_change"))
                .transpose()?,
        })
    }
}

pub struct LuaE {
    lua: Lua,
    config: mlua::Value,
    hook: LuaHook,
}

impl LuaE {
    pub fn init(source: String) -> anyhow::Result<Self> {
        let lua = Lua::new();

        log::debug!("Evaluate lua script");
        let lua_main_table: mlua::Table = lua
            .load(source)
            .eval()
            .context("Failed to evaluate lua script")?;

        let config = lua_main_table.get("config")?;
        let hook = LuaHook::from_lua(lua_main_table.get("hook")?, &lua)?;

        Ok(Self { lua, config, hook })
    }

    pub fn config<C>(&self) -> anyhow::Result<C>
    where
        C: DeserializeOwned + Debug + Send + Sync + 'static,
    {
        let config: C = self.lua.from_value(self.config.clone())?;

        Ok(config)
    }

    pub fn hook(&self) -> &LuaHook {
        &self.hook
    }
}

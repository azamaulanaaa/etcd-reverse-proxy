use std::{fmt::Debug, fs, path::Path};

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

            on_change.call::<()>(args)?;

            return Ok(());
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
    pub fn init(lua_path: &Path) -> anyhow::Result<Self> {
        let source = fs::read_to_string(lua_path).context("Unable to read lua script")?;

        let lua = Lua::new();
        Self::set_secure_package_path(
            &lua,
            lua_path
                .parent()
                .context("Unable to locate lua script's directory")?,
        )?;

        log::debug!("Evaluate lua script");
        let lua_main_table: mlua::Table = lua
            .load(source)
            .eval()
            .context("Failed to evaluate lua script")?;

        let config = lua_main_table.get("config")?;
        let hook = LuaHook::from_lua(lua_main_table.get("hook")?, &lua)?;

        Ok(Self { lua, config, hook })
    }

    fn set_secure_package_path(lua: &Lua, root_dir: &Path) -> anyhow::Result<()> {
        let abs_root_dir = root_dir
            .canonicalize()
            .context("Failed to canonicalize root dir")?;

        let root_path_str = abs_root_dir
            .to_string_lossy()
            .strip_prefix(r"\\?\")
            .context("Path should be valid with prefix removed")?
            .replace('\\', "/");

        let direct_pattern = format!("{}/?.lua", root_path_str);
        let init_pattern = format!("{}/?/init.lua", root_path_str);

        let secure_path = format!("{};{}", direct_pattern, init_pattern);

        let update_path_script = format!("package.path = '{}'", secure_path);

        log::debug!("Set Lua package.path to: {}", secure_path);
        lua.load(&update_path_script).exec()?;

        Ok(())
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

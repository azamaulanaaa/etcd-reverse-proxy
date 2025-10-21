use simple_logger::SimpleLogger;

fn main() -> anyhow::Result<()> {
    SimpleLogger::new().init()?;

    log::info!("Hello, world!");

    Ok(())
}

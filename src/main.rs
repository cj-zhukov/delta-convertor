use app::{handler, Config};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::new()?;
    println!("{}", config);
    handler(config).await?;

    Ok(())
}
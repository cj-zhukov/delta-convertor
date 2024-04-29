use app::{Config, WorkMode, get_aws_client, init, process, register_handlers};

use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let now = Instant::now();
    let config = Config::new()?;
    println!("{}", config);
    if let Some(mode) = WorkMode::new(&config.args.mode) {
        println!("start processing item: {} with mode: {}", &config.item_name, mode.value());
        register_handlers();
        let client = get_aws_client().await;
        match mode {
            WorkMode::Init => init(client, &config).await?,
            WorkMode::Append => process(client, &config).await?,
        }
        println!("end processing item: {} with mode: {} elapsed: {:.2?}", &config.item_name, mode.value(), now.elapsed());
    }

    Ok(())
}
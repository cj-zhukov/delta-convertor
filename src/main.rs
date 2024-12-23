use std::time::Instant;

use color_eyre::Result;

use app::{
    init, 
    process, 
    register_handlers, 
    utils::{backend_config, get_aws_client, Config}, 
    WorkMode,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let now = Instant::now();
    let config = Config::new()?;
    println!("{}", config);
    if let Some(mode) = WorkMode::new(&config.args.mode) {
        println!("start processing item: {} with mode: {}", &config.item_name, mode.value());
        register_handlers();
        let client = get_aws_client(&config.args.region).await;
        let backend_config = backend_config(Some(config.args.region.clone()), None, Some(config.args.dynamo_table_name.clone()));
        match mode {
            WorkMode::Init => init(client, config, backend_config).await?,
            WorkMode::Append => process(client, config, backend_config).await?,
        }
        println!("end processing elapsed: {:.2?}", now.elapsed());
    }

    Ok(())
}
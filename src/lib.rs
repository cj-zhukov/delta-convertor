use std::collections::HashMap;

use aws_sdk_s3::Client;
use table::Table;
use tokio::task::JoinSet;

pub mod table;
pub mod error;
pub use error::DeltaConvertorError;
use utils::{read_file, ASYNC_WORKERS, DEBUG, Config};
pub mod utils;

pub fn register_handlers() {
    deltalake::aws::register_handlers(None);
}

pub async fn init(
    client: Client, 
    config: Config, 
    backend_config: HashMap<String, String>,
) -> Result<(), DeltaConvertorError> {
    let mut stream = client
        .list_objects_v2()
        .bucket(&config.bucket_source)
        .prefix(&config.prefix_source)
        .max_keys(10)
        .into_paginator()
        .send();

    let mut keys = Vec::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for object in objects.contents().iter().cloned() {
            if let Some(key) = object.key {
                if key.ends_with(".parquet") {
                    keys.push(key);
                }
            }
        }
    }

    if let Some(key) = keys.first() {
        let table_uri = format!("s3://{}/{}", &config.bucket_target, &config.prefix_target);
        let partition_columns = &config.args.partition_columns;
        println!("reading parquet file: {}", key);
        let data = read_file(client.clone(), &config.bucket_source, key).await?;
        println!("creating data table from file: {}", key);
        let table = Table::init(data, None).await?;
        println!("creating table: {} prefix: {}", config.item_name, table_uri);
        table.create_delta_table(&table_uri, partition_columns.clone(), backend_config.clone()).await?;
    } else {
        eprintln!("no parquet files found for processing for path: s3://{}/{}", &config.bucket_source, &config.prefix_source);
        return Err(DeltaConvertorError::NoParquetFilesFound);
    }

    Ok(())
}

pub async fn process(
    client: Client, 
    config: Config, 
    backend_config: HashMap<String, String>,
) -> Result<(), DeltaConvertorError> {
    println!("reading keys in prefix: {}", config.prefix_source);
    let mut stream = client
        .list_objects_v2()
        .bucket(&config.bucket_source)
        .prefix(&config.prefix_source)
        .into_paginator()
        .send();

    let mut keys = Vec::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for object in objects.contents().iter().cloned() {
            if let Some(key) = object.key {
                if key.ends_with("parquet") {
                    keys.push(key);
                }
            }
        }
    }

    if config.args.debug.unwrap_or(DEBUG) {
        keys = keys.into_iter().take(10).collect::<Vec<_>>();
    }
    println!("found: {} keys in prefix: {}", keys.len(), config.prefix_source);
    let table_uri = format!("s3://{}/{}", &config.bucket_target, &config.prefix_target);
    let partition_columns = &config.args.partition_columns;
    let checkpoint = &config.args.checkpoint;

    if config.args.workers.unwrap_or(ASYNC_WORKERS) == 1 {
        for key in keys {
            processor(
                client.clone(), 
                config.bucket_source.to_string(),
                table_uri.to_string(),
                key.to_string(),
                partition_columns.clone(),
                backend_config.clone(),
                config.args.chunk_size,
                *checkpoint,
                config.args.debug).await?;
        }
    } else {
        let mut tasks = JoinSet::new();
        let mut outputs = Vec::new();
        for key in keys {
            tasks.spawn(processor(
                client.clone(), 
                config.bucket_source.to_string(),
                table_uri.to_string(),
                key.to_string(),
                partition_columns.clone(),
                backend_config.clone(),
                config.args.chunk_size,
                *checkpoint,
                config.args.debug,
            ));
    
            if tasks.len() == config.args.workers.unwrap_or(ASYNC_WORKERS) {
                outputs.push(tasks.join_next().await);
            }
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(res) => match res {
                    Ok(_) => (),
                    Err(e) => eprintln!("failed writing to delta table: {}", e),
                }
                Err(e) => eprintln!("failed running tokio task: {}", e),
            }
        }
    }

    Ok(())
}

async fn processor(
    client: Client, 
    bucket: String, 
    table_uri: String, 
    key: String, 
    partition_columns: Option<Vec<String>>, 
    backend_config: HashMap<String, String>,
    chunk_size: Option<usize>,
    checkpoint: Option<usize>,
    debug: Option<bool>,
    ) -> Result<(), DeltaConvertorError> {
    println!("reading parquet file: {}", key);
    let data = read_file(client.clone(), &bucket, &key).await?;
    println!("creating data table from file: {}", key);
    let table = Table::new(data, chunk_size).await?;
    if debug.unwrap_or(DEBUG) {
        dbg!(table.metadata());
    }
    println!("writing to delta table from file: {}", key);
    table.to_delta(&table_uri, partition_columns, backend_config, &key, checkpoint).await?;

    Ok(())
}

pub enum WorkMode {
    Append,
    Init, 
}

impl WorkMode {
    pub fn new(name: &str) -> Option<Self> {
        match name {
            "append" => Some(Self::Append),
            "init" => Some(Self::Init),
            _ => {
                println!("unknown mode: {} ", name);
                None
            }
        }
    }

    pub fn value(&self) -> &str {
        match *self {
            Self::Append => "append",
            Self::Init => "init",
        }
    }
}
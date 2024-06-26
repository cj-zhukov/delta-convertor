pub mod error;
pub use error::{Result, Error};

pub mod config;
pub use config::Config;
use tokio::task::JoinSet;

use std::collections::HashMap;
use std::ops::Deref;
use std::io::Cursor;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use tokio_stream::StreamExt;
use arrow::{
    record_batch::RecordBatch,
    datatypes::Schema,
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::transaction::commit_with_retries;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::protocol::checkpoints::create_checkpoint;
use deltalake::kernel::Action;
use deltalake::{open_table_with_storage_options, DeltaTable};
use deltalake::writer::{RecordBatchWriter, DeltaWriter};

pub const AWS_MAX_RETRIES: u32 = 10;
pub const DELTA_MAX_RETRIES: usize = 30; 
pub const ASYNC_WORKERS: usize = 1;
pub const DYNAMO_TABLE: &str = "delta_log";
pub const DEFAULT_REGION: &str = "eu-central-1";
pub const AWS_S3_LOCKING_PROVIDER: &str = "dynamodb";
pub const DEBUG: bool = false;

pub fn register_handlers() {
    deltalake::aws::register_handlers(None);
}

pub async fn init(client: Client, config: Config, backend_config: HashMap<String, String>) -> Result<()> {
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
                if key.ends_with("parquet") {
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
        return Err(Error::Custom("no parquet files found for processing".into()));
    }

    Ok(())
}

pub async fn process(client: Client, config: Config, backend_config: HashMap<String, String>) -> Result<()> {
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
    ) -> Result<()> {
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

pub async fn get_aws_client(region: &str) -> Client {
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(AWS_MAX_RETRIES))
            .build()
    )
}

async fn get_file(client: Client, bucket: &str, key: &str) -> Result<GetObjectOutput> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    Ok(resp)
} 

async fn read_file(client: Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut object = get_file(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }

    Ok(buf)
}

pub fn backend_config(region: Option<String>, provider: Option<String>, table_name: Option<String>) -> HashMap<String, String> {
    HashMap::from([
        ("AWS_REGION".to_string(), region.unwrap_or(DEFAULT_REGION.to_string())),
        ("AWS_S3_LOCKING_PROVIDER".to_string(), provider.unwrap_or(AWS_S3_LOCKING_PROVIDER.to_string())),
        ("DELTA_DYNAMO_TABLE_NAME".to_string(), table_name.unwrap_or(DYNAMO_TABLE.to_string())),
    ])
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

pub struct Table {
    pub schema: Schema,
    pub data: Option<Vec<RecordBatch>>,
    pub rows: Option<usize>,
    pub columns: Option<usize>,
    pub chunk_size: Option<usize>,
}

impl Table {
    pub fn metadata(&self) {
        println!("Number of rows: {}", self.rows.unwrap_or(0));
        println!("Number of columns: {}", self.columns.unwrap_or(0));
        let rec_batch_nums = match &self.data {
            Some(data) => data.len(),
            None => 0,
        };
        println!("Number of record batches: {}", rec_batch_nums);
        println!("Batch size: {}", self.chunk_size.unwrap_or(0));
        println!("Schema: {:?}", &self.schema);
    }

    pub async fn new(data: Vec<u8>, chunk_size: Option<usize>) -> Result<Self> {
        let chunk_size = chunk_size.unwrap_or(1024);
        let cursor = Cursor::new(data);
        let mut reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await?
            .with_batch_size(chunk_size)
            .build()?;

        let schema = reader.schema().deref().clone();

        let mut data: Vec<RecordBatch> = Vec::new();
        let mut rows = 0;
        while let Some(record_batch) = reader.next().await.transpose()? {            
                rows += record_batch.num_rows();
                data.push(record_batch);
        }
        let columns = data[0].num_columns();

        Ok(Self { schema, data: Some(data), rows: Some(rows), columns: Some(columns), chunk_size: Some(chunk_size) })
    }

    pub async fn init(data: Vec<u8>, chunk_size: Option<usize>) -> Result<Self> {
        let chunk_size = chunk_size.unwrap_or(1024);
        let cursor = Cursor::new(data);
        let reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await?
            .with_batch_size(chunk_size)
            .build()?;

        let schema = reader.schema().deref().clone();

        Ok(Self { schema, data: None, rows: None, columns: None, chunk_size: None })
    }
    
    async fn create_delta_table(&self, table_uri: &str, partition_columns: Option<Vec<String>>, backend_config: HashMap<String, String>) -> Result<DeltaTable> {
        let delta_schema = <deltalake::kernel::Schema as TryFrom<&Schema>>::try_from(&self.schema)?;
        let fields = delta_schema.fields().to_owned();
        let builder = match partition_columns {
            Some(cols) => {
                CreateBuilder::new()
                    .with_location(table_uri)
                    .with_storage_options(backend_config)
                    .with_columns(fields)
                    .with_partition_columns(cols)
            },
            None => {
                CreateBuilder::new()
                    .with_location(table_uri)
                    .with_storage_options(backend_config)
                    .with_columns(fields)
            }
        };
        
        Ok(builder.await?)
    }

    pub async fn to_delta(&self, table_uri: &str, partition_columns: Option<Vec<String>>, backend_config: HashMap<String, String>, key: &str, checkpoint: Option<usize>) -> Result<()> {
        if let Some(record_batches) = &self.data {
            let mut writer = RecordBatchWriter::try_new(table_uri, Arc::new(self.schema.clone()), partition_columns.clone(), Some(backend_config.clone()))?;
            let mut delta_table = open_table_with_storage_options(table_uri, backend_config.clone()).await?;

            for record_batch in record_batches {
                writer.write(record_batch.clone()).await?;
                
                let action = writer
                    .flush()
                    .await?
                    .iter() 
                    .map(|add| {
                        let clone = add.clone();
                        Action::Add(clone)
                    })
                    .collect::<Vec<_>>();
                
                let operation = DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: partition_columns.clone(), 
                    predicate: None,
                };

                println!("commiting transaction for delta table key: {}", key);
                let snap = delta_table.snapshot()?;
                let version = commit_with_retries(delta_table.log_store().as_ref(), &action, operation, Some(snap), None, DELTA_MAX_RETRIES).await?;
                delta_table.update().await?;
                println!("delta table version: {} for key: {}", version, key);

                if let Some(checkpoint) = checkpoint {
                    if delta_table.version() % checkpoint as i64 == 0 {
                        println!("creating checkpoint for delta table");
                        create_checkpoint(&delta_table).await?;
                    }
                }
            }
        } else {
            println!("no data for writing found for delta table");
        }
        
        Ok(())
    }
}
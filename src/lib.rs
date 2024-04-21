pub mod config;
pub use config::Config;
use tokio::task::JoinSet;

use std::time::Instant;
use std::collections::HashMap;
use std::ops::Deref;
use std::io::Cursor;
use std::sync::Arc;

use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use tokio_stream::StreamExt;
use anyhow::Context;
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

const AWS_MAX_RETRIES: u32 = 10;
const DELTA_MAX_RETRIES: usize = 30; 
const ASYNC_WORKERS: usize = 1;
const DEBUG: bool = false;

pub async fn handler(config: Config) -> anyhow::Result<()> {
    if let Some(mode) = WorkMode::new(&config.args.mode) {
        deltalake::aws::register_handlers(None);
        let now = Instant::now();
        println!("start processing item: {}", &config.item_name);
        let client = get_aws_client().await?;

        match mode {
            WorkMode::Init => {
                println!("processing mode: {}", mode.value());
                init(client, &config).await?;
            }
            WorkMode::Append => {
                println!("processing mode: {}", mode.value());
                process(client, &config).await?;
            }
        }

        println!("end processing item: {} elapsed: {:.2?}", &config.item_name, now.elapsed());
    }

    Ok(())
}

async fn init(client: Client, config: &Config) -> anyhow::Result<()> {
    let mut stream = client
        .list_objects_v2()
        .bucket(&config.bucket_source)
        .prefix(&config.prefix_source)
        .max_keys(10)
        .into_paginator()
        .send();

    let mut keys = Vec::new();
    while let Some(output) = stream.next().await {
        let objects = output.context(format!("could not get output bucket={} prefix={}", config.bucket_source, config.prefix_source))?;
        for object in objects.contents().to_owned() {
            if let Some(key) = object.key {
                if key.ends_with("parquet") {
                    keys.push(key);
                }
            }
        }
    }

    if let Some(key) = keys.get(0) {
        let backend_config = backend_config(None, None, None);
        let table_uri = format!("s3://{}/{}", &config.bucket_target, &config.prefix_target);
        let partition_columns = &config.args.partition_columns;
        println!("reading parquet file: {}", key);
        let data = read_file(client.clone(), &config.bucket_source, key).await?;
        println!("creating data table from file: {}", key);
        let table = Table::init(data, None).await?;
        println!("creating table: {} prefix: {}", config.item_name, table_uri);
        table.create_delta_table(&table_uri, partition_columns.clone(), backend_config.clone()).await?;
    }

    Ok(())
}

async fn process(client: Client, config: &Config) -> anyhow::Result<()> {
    println!("reading keys in prefix={}", config.prefix_source);
    let mut stream = client
        .list_objects_v2()
        .bucket(&config.bucket_source)
        .prefix(&config.prefix_source)
        .into_paginator()
        .send();

    let mut keys = Vec::new();
    while let Some(output) = stream.next().await {
        let objects = output.context(format!("could not get output bucket={} prefix={}", config.bucket_source, config.prefix_source))?;
        for object in objects.contents().to_owned() {
            if let Some(key) = object.key {
                if key.ends_with("parquet") {
                    keys.push(key);
                }
            }
        }
    }

    if config.args.debug.unwrap_or(DEBUG) == true {
        keys = keys.into_iter().take(10).collect::<Vec<_>>();
    }
    println!("found: {} keys in prefix: {}", keys.len(), config.prefix_source);
    let backend_config = backend_config(None, None, None);
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
                    Err(e) => println!("could not write to delta table: {}", e),
                }
                Err(e) => println!("could not run task: {}", e),
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
    ) -> anyhow::Result<()> {
    println!("reading parquet file: {}", key);
    let data = read_file(client.clone(), &bucket, &key).await?;
    println!("creating data table from file: {}", key);
    let table = Table::new(data, chunk_size).await?;
    if debug.unwrap_or(DEBUG) == true {
        dbg!(table.metadata());
    }
    println!("writing to delta table from file: {}", key);
    table.to_delta(&table_uri, partition_columns, backend_config, &key, checkpoint).await?;

    Ok(())
}

async fn get_aws_client() -> anyhow::Result<Client> {
    let region_provider = RegionProviderChain::default_provider().or_else("eu-central-1");
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09()).region(region_provider).load().await;
    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(AWS_MAX_RETRIES))
            .build()
    );

    Ok(client)
}

async fn get_file(client: Client, bucket: &str, key: &str) -> anyhow::Result<GetObjectOutput> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("could not get object output for bucket: {} key: {}", bucket, key))?;

    Ok(resp)
} 

async fn read_file(client: Client, bucket: &str, key: &str) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut object = get_file(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await.context(format!("could not read data from stream bucket: {} key: {}", bucket, key))? {
        buf.extend(bytes.to_vec());
    }

    Ok(buf)
}

fn backend_config(region: Option<&str>, provider: Option<&str>, table_name: Option<&str>) -> HashMap<String, String> {
    HashMap::from([
        ("AWS_REGION".to_string(), region.unwrap_or("eu-central-1").to_string()),
        ("AWS_S3_LOCKING_PROVIDER".to_string(), provider.unwrap_or("dynamodb").to_string()),
        ("DELTA_DYNAMO_TABLE_NAME".to_string(), table_name.unwrap_or("delta_log").to_string()),
    ])
}

pub enum WorkMode {
    Append,
    Init, 
}

impl WorkMode {
    fn new(name: &str) -> Option<Self> {
        match name {
            "append" => Some(Self::Append),
            "init" => Some(Self::Init),
            _ => {
                println!("unknown mode: {} ", name);
                None
            }
        }
    }

    fn value(&self) -> &str {
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

    pub async fn new(data: Vec<u8>, chunk_size: Option<usize>) -> anyhow::Result<Self> {
        let chunk_size = chunk_size.unwrap_or(1024);
        let cursor = Cursor::new(data);
        let mut reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await
            .context(format!("could not create async parquet reader"))?
            .with_batch_size(chunk_size)
            .build()
            .context(format!("could not create async parquet stream"))?;

        let schema = reader.schema().deref().clone();

        let mut data: Vec<RecordBatch> = Vec::new();
        let mut rows = 0;
        while let Some(maybe_batch) = reader.next().await {
            let record_batch = maybe_batch.context(format!("could not get record_batch"))?;                
                rows += record_batch.num_rows();
                data.push(record_batch);
        }
        let columns = data[0].num_columns();

        Ok(Self { schema, data: Some(data), rows: Some(rows), columns: Some(columns), chunk_size: Some(chunk_size) })
    }

    pub async fn init(data: Vec<u8>, chunk_size: Option<usize>) -> anyhow::Result<Self> {
        let chunk_size = chunk_size.unwrap_or(1024);
        let cursor = Cursor::new(data);
        let reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await
            .context(format!("could not create async parquet reader"))?
            .with_batch_size(chunk_size)
            .build()
            .context(format!("could not create async parquet stream"))?;

        let schema = reader.schema().deref().clone();

        Ok(Self { schema, data: None, rows: None, columns: None, chunk_size: None })
    }
    
    async fn create_delta_table(&self, table_uri: &str, partition_columns: Option<Vec<String>>, backend_config: HashMap<String, String>) -> anyhow::Result<DeltaTable> {
        let delta_schema = <deltalake::kernel::Schema as TryFrom<&Schema>>::try_from(&self.schema)
            .context("failed to convert arrow schema to delta schema")?;

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
        
        Ok(builder.await.context("could not create delta table")?)
    }

    pub async fn to_delta(&self, table_uri: &str, partition_columns: Option<Vec<String>>, backend_config: HashMap<String, String>, key: &str, checkpoint: Option<usize>) -> anyhow::Result<()> {
        if let Some(record_batches) = &self.data {
            let mut writer = RecordBatchWriter::try_new(table_uri, Arc::new(self.schema.clone()), partition_columns.clone(), Some(backend_config.clone()))
                .context(format!("could not create writer for delta table for key: {}", key))?;

            let mut delta_table = open_table_with_storage_options(table_uri, backend_config.clone())
                .await
                .context(format!("could not open delta table: {} for key: {}", &table_uri, key))?;

            for record_batch in record_batches {
                writer.write(record_batch.clone())
                    .await
                    .context(format!("could not write to writer for key: {}", key))?; 
                
                let action = writer
                    .flush()
                    .await
                    .context(format!("could not flush the writer for key: {}", key))?
                    .iter() 
                    .map(|add| {
                        let clone = add.clone();
                        Action::Add(clone.into())
                    })
                    .collect::<Vec<_>>();
                
                let operation = DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: partition_columns.clone(), 
                    predicate: None,
                };

                println!("commiting transaction for delta table key: {}", key);
                let snap = delta_table.snapshot().context(format!("could not get snapshot for delta table for key: {}", key))?;
                let version = commit_with_retries(delta_table.log_store().as_ref(), &action, operation, Some(snap), None, DELTA_MAX_RETRIES)
                    .await
                    .context(format!("could not commit transaction for delta table for key: {}", key))?;

                delta_table.update()
                    .await
                    .context(format!("could not update delta table for key: {}", key))?;

                println!("delta table version: {} for key: {}", version, key);

                if let Some(checkpoint) = checkpoint {
                    if delta_table.version() % checkpoint as i64 == 0 {
                        println!("creating checkpoint for delta table");
                        create_checkpoint(&delta_table).await.context(format!("could not create checkpoint for delta for key: {}", key))?;
                    }
                }
            }
        } else {
            println!("no data to write found for delta table");
        }
        
        Ok(())
    }
}
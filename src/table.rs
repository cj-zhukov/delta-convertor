use std::collections::HashMap;
use std::ops::Deref;
use std::io::Cursor;
use std::sync::Arc;

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

use crate::error::DeltaConvertorError;
use crate::utils::DELTA_MAX_RETRIES;

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

    pub async fn new(data: Vec<u8>, chunk_size: Option<usize>) -> Result<Self, DeltaConvertorError> {
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

    pub async fn init(data: Vec<u8>, chunk_size: Option<usize>) -> Result<Self, DeltaConvertorError> {
        let chunk_size = chunk_size.unwrap_or(1024);
        let cursor = Cursor::new(data);
        let reader = ParquetRecordBatchStreamBuilder::new(cursor)
            .await?
            .with_batch_size(chunk_size)
            .build()?;

        let schema = reader.schema().deref().clone();

        Ok(Self { schema, data: None, rows: None, columns: None, chunk_size: None })
    }
    
    pub async fn create_delta_table(
        &self, 
        table_uri: &str, 
        partition_columns: Option<Vec<String>>, 
        backend_config: HashMap<String, String>,
    ) -> Result<DeltaTable, DeltaConvertorError> {
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

    pub async fn to_delta(
        &self, 
        table_uri: &str, 
        partition_columns: Option<Vec<String>>, 
        backend_config: HashMap<String, String>, 
        key: &str, 
        checkpoint: Option<usize>,
    ) -> Result<(), DeltaConvertorError> {
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
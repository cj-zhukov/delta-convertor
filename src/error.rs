pub type Result<T> = core::result::Result<T, Error>;

use thiserror::Error;
use std::env::VarError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_types::byte_stream::error::Error as AwsSmithyError;
use parquet::errors::ParquetError;
use deltalake::arrow::error::ArrowError;
use deltalake::{DeltaTableError, protocol::ProtocolError};

#[derive(Error, Debug)]
pub enum Error {
    #[error("custom error: `{0}`")]
    Custom(String),

    #[error("var error: `{0}`")]
    VarError(#[from] VarError),

    #[error("io error: `{0}`")]
    IOError(#[from] std::io::Error),

    #[error("config parse error: `{0}`")]
    ConfigParseError(#[from] serde_json::Error),

    #[error("aws_sdk_s3 error: `{0}`")]
    AwsSdkS3Error(#[from] aws_sdk_s3::Error),

    #[error("listing object error: {0}")]
    ListObjectError(#[from] SdkError<ListObjectsV2Error>),

    #[error("get object error: {0}")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("put object error: {0}")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("byte stream error: {0}")]
    ByteSreamError(#[from] AwsSmithyError),
    
    #[error("parquet error: `{0}`")]
    ParquetError(#[from] ParquetError),

    #[error("arrow error: `{0}`")]
    ArrowError(#[from] ArrowError),

    #[error("delta table error: `{0}`")]
    DeltatableError(#[from] DeltaTableError),

    #[error("delta table protocol error: `{0}`")]
    DeltaprotocolError(#[from] ProtocolError),
}
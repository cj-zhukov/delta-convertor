use std::env::VarError;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_smithy_types::byte_stream::error::Error as AwsSmithyError;
use color_eyre::Report;
use parquet::errors::ParquetError;
use deltalake::arrow::error::ArrowError;
use deltalake::{DeltaTableError, protocol::ProtocolError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeltaConvertorError {
    #[error("VarError")]
    VarError(#[from] VarError),

    #[error("IOError")]
    IOError(#[from] std::io::Error),

    #[error("ConfigParseError")]
    ConfigParseError(#[from] serde_json::Error),

    #[error("AwsSdkS3Error")]
    AwsSdkS3Error(#[from] aws_sdk_s3::Error),

    #[error("ListObjectError")]
    ListObjectError(#[from] SdkError<ListObjectsV2Error>),

    #[error("GetObjectError")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("PutObjectError")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("AwsSmithyError")]
    ByteSreamError(#[from] AwsSmithyError),

    #[error("NoParquetFilesFound")]
    NoParquetFilesFound,
    
    #[error("ParquetError")]
    ParquetError(#[from] ParquetError),

    #[error("ArrowError")]
    ArrowError(#[from] ArrowError),

    #[error("DeltaTableError")]
    DeltaTableError(#[from] DeltaTableError),

    #[error("DeltaProtocolError")]
    DeltaProtocolError(#[from] ProtocolError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}
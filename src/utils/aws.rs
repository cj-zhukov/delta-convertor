use crate::error::DeltaConvertorError;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;

use super::AWS_MAX_RETRIES;

pub async fn get_aws_client(region: &str) -> Client {
    let config = aws_config::defaults(BehaviorVersion::latest())
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

async fn get_file(client: Client, bucket: &str, key: &str) -> Result<GetObjectOutput, DeltaConvertorError> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    Ok(resp)
} 

pub async fn read_file(client: Client, bucket: &str, key: &str) -> Result<Vec<u8>, DeltaConvertorError> {
    let mut buf = Vec::new();
    let mut object = get_file(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }

    Ok(buf)
}
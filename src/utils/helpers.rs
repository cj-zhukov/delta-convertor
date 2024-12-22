use std::collections::HashMap;

use super::{AWS_S3_LOCKING_PROVIDER, DEFAULT_REGION, DYNAMO_TABLE};

pub fn backend_config(
    region: Option<String>, 
    provider: Option<String>, 
    table_name: Option<String>,
) -> HashMap<String, String> {
    HashMap::from([
        ("AWS_REGION".to_string(), region.unwrap_or(DEFAULT_REGION.to_string())),
        ("AWS_S3_LOCKING_PROVIDER".to_string(), provider.unwrap_or(AWS_S3_LOCKING_PROVIDER.to_string())),
        ("DELTA_DYNAMO_TABLE_NAME".to_string(), table_name.unwrap_or(DYNAMO_TABLE.to_string())),
    ])
}
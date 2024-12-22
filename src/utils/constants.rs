pub const AWS_MAX_RETRIES: u32 = 10;
pub const DELTA_MAX_RETRIES: usize = 30; 
pub const ASYNC_WORKERS: usize = 1;
pub const DYNAMO_TABLE: &str = "delta_log";
pub const DEFAULT_REGION: &str = "eu-central-1";
pub const AWS_S3_LOCKING_PROVIDER: &str = "dynamodb";
pub const DEBUG: bool = false;
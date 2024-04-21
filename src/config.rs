use std::fmt::Debug;
use std::env;

use anyhow::Context;
use serde::Deserialize;

struct Input {
    bucket_source: String,
    bucket_target: String,
    prefix_source: String,
    prefix_target: String,
    item_name: String,
    args: String,
}

impl Input {
    fn new() -> anyhow::Result<Self> {
        let bucket_source = env::var("bucket_source")?;
        let bucket_target = env::var("bucket_target")?;
        let prefix_source = env::var("prefix_source")?;
        let prefix_target = env::var("prefix_target")?;
        let item_name = env::var("item_name")?;
        let args = env::var("args")?;

        Ok(Self { bucket_source, bucket_target, prefix_source, prefix_target, item_name, args })
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    pub bucket_source: String,
    pub bucket_target: String,
    pub prefix_source: String,
    pub prefix_target: String,
    pub item_name: String, // delta table name 
    pub args: Args,
}

#[derive(Deserialize, Debug)]
pub struct Args {
    pub partition_columns: Option<Vec<String>>, // delta table is pationed by these columns
    pub workers: Option<usize>, // async workers/files count
    pub chunk_size: Option<usize>,  // size of recordbatch, 1024 is default
    pub mode: String, // delta-convertor mode init or append
    pub checkpoint: Option<usize>, // create checkpoint after n delta version
    pub debug: Option<bool>, // testing process n files 
}

impl Config {
    pub fn new() -> anyhow::Result<Self> {
        let input = Input::new().context("could not get input for config")?;
        let args: Args = serde_json::from_str(&input.args)?;

        Ok(Self { 
            bucket_source: input.bucket_source, 
            bucket_target: input.bucket_target, 
            prefix_source: input.prefix_source, 
            prefix_target: input.prefix_target, 
            item_name: input.item_name, 
            args  
        })
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config: item_name={} bucket_source={} bucket_target={} prefix_source={} prefix_target={} workers={:?} partitions={:?} chunk_size={:?} checkpoint={:?} mode={:?} debug={:?}",
        self.item_name, 
        self.bucket_source, 
        self.bucket_target, 
        self.prefix_source, 
        self.prefix_target, 
        self.args.workers,
        self.args.partition_columns,
        self.args.chunk_size,
        self.args.checkpoint,
        self.args.mode,
        self.args.debug,
        )
    }
}

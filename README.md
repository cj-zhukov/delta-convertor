# delta-convertor
delta-convertor is a Rust library for converting parquet files from Data Lake in AWS S3 into Delta Lake.

## Description
delta-convertor reads parquet files from provided source, converts them to delta format and writes into delta table.
delta-convertor converts schema of parquet files into delta schema automatically. 
Delta tables can be available in AWS Glue catalog and can be queried with AWS Athena.
Delta tables can be queried with spark in DataBricks. 

## Installation
Use the package manager cargo or docker to install delta-convertor.

## Modes overview
init: initialization of empty delta table (you will see only _delta_log folder)
append: read parquet files, convert to delta and write to delta table

## How to use with AWS
1) run delta-convertor with mode="init" for initialization empty delta table in AWS S3
2) run AWS Crawler with delta source option for creating delta table in AWS Glue catalog (need to do it only once)
3) run delta-convertor with mode="append" for writing parquet files from Data Lake to Delta Lake

## How to read delta table with DataBricks
```python
df = spark.read.format("delta").load("path/to/data/")
display(df.limit(100))
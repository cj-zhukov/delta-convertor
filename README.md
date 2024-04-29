Convert parquet files from Data Lake in AWS S3 into Delta Lake

Motivation:
you want to have Delta tables in AWS Glue catalog available for quering

How to use:
1) run delta-convertor with mode="init" for initialization empty delta table in AWS S3 (you will see only delta_log folder)
2) run AWS Crawler with delta source option for creating delta table in AWS Glue catalog (need to do it only once)
3) run delta-convertor with mode="append" for writing parquet files from Data Lake to Delta Lake
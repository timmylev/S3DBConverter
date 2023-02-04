# S3DBConverter
A micro-service that converts S3DB data (Transmuter output) to various formats, compressions, and partitions.
* formats: arrow, parquet
* compressions: br, bz2, gz, lz4, zst, sz
* partition sizes: hour, day, month, year
* directory structure:
  * dataclient - Uses the same directory structure as the source S3DB data.
  * athena - Uses Apache Hive partitioning scheme. Arrow format is not supported.

Also provides a CLI util to:
* trigger custom data conversion jobs
* manage the Glue Data Catalog used by Athena

## Limitations
1. The destination/output for converted files must be **in the same bucket** (`invenia-datafeeds-output`), but with a **non-overlapping prefix with the source** (`version5/aurora/gz/`). You can copy the converted output to any other bucket on your own.
2. **Partitioning by year will fail** for very large datasets such as CAISO Price Data due to AWS Lambda hitting max memory (10GB). We'll need to look into alternatives such as AWS Fargate or Batch if generating yearly partitions are really desired for all datasets.
3. There is no way to add live jobs via the provided CLI util, a stack update is required to add live jobs. Read the next section for more details.

## Usage
A stack with the name `S3DBConverter` has already been deployed to the production account.
Simply run the `s3dbcli.py` script provided in the repo to start up the `S3DB CLI` and follow the guided prompts.
Be sure to assume the prod account role beforehand as you will be interacting with AWS resources in the prod account.
```
pip install -r requirements.txt

export AWS_DEFAULT_PROFILE=production:admin
python s3dbcli.py
```
Data conversion jobs are one-off operations, they will not automatically trigger on new prod data.
To set up a new automated converter for live data, add a config entry to the `lambdas/prod_listener.py` function and update the prod stack.
Live data conversions are currently only supported for hourly/daily partitions.

The `S3DB CLI` also provides utils to keep the AWS Glue Data Catalog up to date with S3DB data.
This catalog contains metadata that is used by AWS Athena when processing user queries.
These operations only modify the Glue Data Catalog, not the S3 data itself:
* Checking for new S3DB datasets and registering them as new tables in the Glue catalog
* Updating the Glue catalog when a schema change for a dataset in S3DB is detected
* Removing registered tables from the Glue catalog

## Deploy / Update
CFN args like stack name, bucket name, bucket prefix, etc. are already hard coded as constant in `deploy.py`, so, simply run the script to update (or redeploy) the stack:
```
export AWS_DEFAULT_PROFILE=production:admin
python deploy.py
```

## Architecture
![S3DBConverter Architecture Diagram](./S3DBConverters.drawio.svg)

Notes:
* There are two types of workloads: live-fills and back-fills.
    * Live-fill workloads are automatically triggered as new files in prod are created/updated. S3DBConverter only subscribes to the prod bucket/prefix (`s3://invenia-datafeeds-output/version5/aurora/gz/`). The Request Generator (live, lambdas/prod_listener.py) generates pre-defined jobs and sends it off to the next stage. Currently, only hour/day partitions are supported for live-fill workloads.
    * Back-fill workloads are one-off jobs triggered manually by users via the `trigger.py` CLI. The Request Generator (backfill, aka lambdas/request_generator.py) generates user-defined jobs and sends it off to the next stage. Back-fill workloads support all partition sizes.
* There are two types of jobs: single-file jobs (hour/day partition) and batch-file jobs (month/year partition).
    * Single-file jobs are jobs that involve only a single input file. Currently, Datafeeds uses a daily (24h) partition, so jobs that do hour/day partitions are single-file jobs.
    * Batch-file jobs are jobs that involve multiple input files. Currently, Datafeeds uses a daily (24h) partition, so jobs that do month or year partitions are batch-file jobs.
* Both job handlers (lambda functions) for the single-file and batch-file jobs actually run the same code (lambdas/request_handler.py), the only difference is the batch-file lambda function is allocated more RAM.

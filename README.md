# S3DBConverter
S3DB data format converter:
* formats: arrow
* partition options: day, month, year
* compression options: br, bz2, gz, lz4, zst, sz

**WARNING:** Partitioning by year will fail for very large datasets such as CAISO Price Data due to AWS Lambda hitting max memory (10GB). We'll need to look into alternative such as AWS Fargate or Batch if generating yearly partition are really desired for all datasets.

## Limitations
1. Formats other than arrow are not yet supported, it should be fairly easy to add support for different formats.
2. The destination/output for converted files must be in the same bucket (`invenia-datafeeds-output`), but with a non-overlapping prefix with the source (`version5/aurora/gz/`). You can copy the converted output to any other bucket on your own.

## Usage
A stack with the name `S3DBConverter` has already been deployed to the prod account, simply run the `trigger.py` script and follow the guided interactive CLI prompts.
Be sure to assume the prod account role beforehand.
```
pip install -r requirements-trigger.txt

export AWS_DEFAULT_PROFILE=production:admin
python trigger.py
```
Note that datasets conversions jobs are a one-time operation and it will not automatically receive new prod data.
To set up a new automated converter for live data, add a configuration to the `lambdas/prod_listener.py` function and update the prod stack.
Live conversions are currently only supported for daily partitions (same partition as prod).
Live re-partitioning of data (into month / year partitions) are not yet supported.

## Deploy / Update
CFN args like stack name, bucket name, bucket prefix, etc. are already hard coded as constant in `deploy.py`, so, simply run the script to update (or redeploy) the stack:
```
export AWS_DEFAULT_PROFILE=production:admin
python deploy.py
```

## Architecture
![S3DBConverter Architecture Diagram](./S3DBConverters.drawio.svg)

Notes:
* There are two types of workloads: live and backfill.
    * Live workloads are automatically triggered as new files in prod are created/updated. S3DBConverter only subscribes to the prod bucket/prefix (`s3://invenia-datafeeds-output/version5/aurora/gz/`). The Request Generator (live) generates pre-defined jobs and sends it off to the next stage.
    * Backfill workloads are one-off jobs triggered manually by users. The Request Generator (backfill) generates user-defined jobs and sends it off to the next stage.
* There are two types of jobs: single-file jobs and batch-file jobs.
    * Single-file jobs are jobs to convert a single input file into a single output file. Currently, Datafeeds uses a daily (24h) partition, so jobs that do daily partitions are single-file jobs.
    * Batch-file jobs are jobs to convert a multiple input files into a single output file. Currently, Datafeeds uses a daily (24h) partition, so jobs that do monthly or yearly partitions are batch-file jobs.
* Both job handlers (lambda functions) for the single-file and batch-file jobs actually run the same code, the only difference being the amount of memory allocated to the function.

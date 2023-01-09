# S3DBConverter
S3DB data format converter:
* formats: arrow
* partition options: day, month, year
* compression options: br, bz2, gz, lz4, zst, sz

**WARNING:** Partitioning by year will fail for very large datasets such as CAISO Price Data due to AWS Lambda hitting max memory (10GB). We'll need to look into alternative such as AWS Fargate or Batch if yearly partition are really desired for all datasets.

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
Note that generated/converted datasets is a one-time operation and it will not automatically receive new prod data.
To set up a new automated converter for live data, add a configuration to the `lambdas/prod_listener.py` function and update the prod stack.

## Deploy / Update
CFN args like stack name, bucket name, bucket prefix, etc. are already hard coded as constant in `deploy.py`, so, simply run the script to update (or redeploy) the stack:
```
export AWS_DEFAULT_PROFILE=production:admin
python deploy.py
```

# S3DBConverter
S3DB data format converter:
* formats: arrow
* partition options: day, month, year
* compression options: br, bz2, gz, lz4, zst, sz

**WARNING:** Partitioning by year will fail for very large datasets such as CAISO Price Data due to AWS Lambda hitting max memory (10GB). We'll need to look into alternative such as AWS Fargate or Batch for this.

## Limitations
1. Formats other than arrow are not yet supported.
2. The destination/output must be in the same bucket (`invenia-datafeeds-output`), but with a non-overlapping prefix with the source (`version5/aurora/gz/`).

## Usage
A stack with the name `S3DBConverter` has already been deployed to the prod account, simply run the `trigger.py` scripts and follow the prompts.
Be sure to assume the prod account role beforehand.
```
pip install -r requirements-trigger.txt

export AWS_DEFAULT_PROFILE=production:admin
python trigger.py
```

## Deploy / Update
```
export AWS_DEFAULT_PROFILE=production:admin
python deploy.py
```

## TODO:
1. prod listeners
2. convert lists, tuples, and bools
3. support parquet
4. concurrent downloads

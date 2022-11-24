# S3DBConverter
S3DB data format converter.

## Limitations
1. Re-partitioning of data/files is not yet supported.
2. Outputs other than `arrow.zst` are not yet supported.
3. The destination/output must be in the same bucket (`invenia-datafeeds-output`), but with a non-overlapping prefix with the source (`version5/aurora/gz/`).

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

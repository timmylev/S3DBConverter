import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
from plz import build_zip


PY_VER = "3.9"

# base paths
BASE_DIR = Path(__file__).absolute().parent

# build paths
BUILD_DIR = BASE_DIR / "build"
LAMBDA_DIR = BASE_DIR / "lambdas"
LAMBDA_REQ = BASE_DIR / "requirements-lambda.txt"

CFN_TEMPLATE = BASE_DIR / "template.yaml"

CODE_BUCKET = "invenia-datafeeds-code"
STACK_NAME = "S3DBConverter"

S3DB_BUCKET = "invenia-datafeeds-output"
S3DB_BUCKET_PREFIX = "version5/aurora/gz/"
S3DB_BUCKET_SNS = "arn:aws:sns:us-east-1:516256908252:invenia-datafeeds-output-topic-BucketTopic-AHM26GG3AE5J"  # noqa


def main():
    print("Building Lambda Bundle...")
    zip_file = build_zip(
        BUILD_DIR, LAMBDA_DIR, requirements=LAMBDA_REQ, python_version=PY_VER
    )

    key = upload_lambda_bundle(zip_file, CODE_BUCKET)

    create_stack(STACK_NAME, CODE_BUCKET, key)


def upload_lambda_bundle(zip_path, bucket):
    dt_now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    key = f"S3DBConverter-{dt_now}.zip"

    print(f"Uploading Lambda Bundle to 's3://{bucket}/{key}'...")
    boto3.client("s3").upload_file(str(zip_path), bucket, key)

    return key


def create_stack(stackname, bucket, key):
    print("Triggering stack creation/update...")
    client = boto3.client("cloudformation")

    args = {
        "StackName": stackname,
        "TemplateBody": CFN_TEMPLATE.read_text(),
        "Parameters": [
            {"ParameterKey": "PythonVersion", "ParameterValue": f"python{PY_VER}"},
            {"ParameterKey": "CodeS3Bucket", "ParameterValue": bucket},
            {"ParameterKey": "CodeS3Key", "ParameterValue": key},
            {"ParameterKey": "S3DBBucket", "ParameterValue": S3DB_BUCKET},
            {"ParameterKey": "S3DBProdPrefix", "ParameterValue": S3DB_BUCKET_PREFIX},
            {"ParameterKey": "S3DBBucketSNS", "ParameterValue": S3DB_BUCKET_SNS},
            {"ParameterKey": "S3DBBucketSNSFilter", "ParameterValue": sns_filter()},
        ],
        "Capabilities": ["CAPABILITY_NAMED_IAM"],
    }

    try:
        client.create_stack(**args)
        opr = "create"
    except client.exceptions.AlreadyExistsException:
        client.update_stack(**args)
        opr = "update"

    print(f"Waiting on {opr} completion... killing this won't do anything")
    client.get_waiter(f"stack_{opr}_complete").wait(StackName=stackname)
    print("Done")


def sns_filter():
    policy = {
        "S3Key": [{"prefix": S3DB_BUCKET_PREFIX}],
        "EventName": [{"prefix": "ObjectCreated"}],
    }
    return json.dumps(policy)


if __name__ == "__main__":
    main()

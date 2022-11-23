from urllib.parse import unquote

import boto3
from common import migrate_file


S3_CLIENT = boto3.client("s3")


def lambda_handler(event, context):
    for message in event["Records"]:
        s3_key = unquote(message["body"])
        attrs = message["messageAttributes"]

        compression = attrs["compression"]["stringValue"]
        dest_prefix = attrs["dest_prefix"]["stringValue"]

        migrate_file(s3_key, dest_prefix, compression)

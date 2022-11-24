import json
from urllib.parse import unquote

import boto3

from lambdas.common import migrate_to_arrow


S3_CLIENT = boto3.client("s3")


def lambda_handler(event, context):
    print(event)

    for message in event["Records"]:
        event = json.loads(unquote(message["body"]))

        if "s3_key" in event:
            s3_key = event["s3_key"]
            compression = event["compression"]
            dest_prefix = event["dest_prefix"]

            migrate_to_arrow(s3_key, dest_prefix, compression)

        else:
            raise Exception(f"Invalid event: {event}")

import json
import os
from urllib.parse import unquote

import boto3

from lambdas.common import migrate_to_arrow


S3_CLIENT = boto3.client("s3")


def lambda_handler(event, context):
    print(event)

    for message in event["Records"]:
        event = json.loads(unquote(message["body"]))

        compression = event["compression"]
        level = event.get("compression_level")
        dest_prefix = event["dest_prefix"]

        if "s3_key" in event:
            s3keys = [event["s3_key"]]
            _, filename = event["s3_key"].rsplit("/", 1)
            file_start = int(filename.split(".")[0])

        else:
            file_start = event["file_start"]
            s3key_prefix = event["s3key_prefix"]
            s3key_suffixes = event["s3key_suffixes"]
            s3keys = [os.path.join(s3key_prefix, i) for i in s3key_suffixes]

        migrate_to_arrow(s3keys, file_start, dest_prefix, compression, level)

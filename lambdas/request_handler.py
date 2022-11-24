import json
from urllib.parse import unquote

import boto3

from lambdas.common import SOURCE_PREFIX, get_dataset_pkeys, to_arrow


S3_CLIENT = boto3.client("s3")


def lambda_handler(event, context):
    print(event)

    for message in event["Records"]:
        event = json.loads(unquote(message["body"]))

        if "s3_key" in event:
            s3_key = event["s3_key"]
            compression = event["compression"]
            dest_prefix = event["dest_prefix"]

            if "pkeys" in event:
                pkeys = event["pkeys"]
            else:
                coll, ds, _ = s3_key.removeprefix(SOURCE_PREFIX).split("/", 2)
                pkeys = get_dataset_pkeys(coll, ds)

            to_arrow(s3_key, pkeys, dest_prefix, compression)

        else:
            raise Exception(f"Invalid event: {event}")

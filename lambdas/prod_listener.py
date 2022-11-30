import json
import os
from urllib.parse import unquote

import boto3


SQS_CLIENT = boto3.client("sqs")
SQS_BATCH_SIZE = 10
SINGLE_JOB_SQS_URL = os.environ["SINGLE_JOB_SQS_URL"]

DEST_STORES = [
    {
        "dest_prefix": "version5/arrow/zst_lv22/day/",
        "compression": "zst",
        "compression_level": 22,
    },
]


def lambda_handler(event, context):
    print(f"Event: {event}")

    for message in event["Records"]:
        sns_event = json.loads(message["body"])
        s3_event = json.loads(unquote(sns_event["Message"]))

        s3_key = s3_event["s3"]["object"]["key"]

        for dest in DEST_STORES:
            SQS_CLIENT.send_message(
                QueueUrl=SINGLE_JOB_SQS_URL,
                MessageBody=json.dumps({"s3_key": s3_key, **dest}),
            )

import os

import boto3
from common import batch_items, list_source_keys


SQS_URL = os.environ["SQS_URL"]
SQS_CLIENT = boto3.client("sqs")
SQS_BATCH_SIZE = 10


def lambda_handler(event, context):
    print(f"Event: {event}")
    attrs = {
        "compression": {"StringValue": event["compression"], "DataType": "String"},
        "dest_prefix": {"StringValue": event["dest_prefix"], "DataType": "String"},
    }

    for batch in batch_items(list_source_keys(), SQS_BATCH_SIZE):
        SQS_CLIENT.send_message_batch(
            QueueUrl=SQS_URL,
            Entries=[
                {"Id": i, "MessageBody": k, "MessageAttributes": attrs}
                for i, k in enumerate(batch)
            ],
        )

import json
import os
from urllib.parse import unquote

from lambdas.common import SOURCE_PREFIX, SQS_CLIENT, copy_metadata_file


SQS_BATCH_SIZE = 10
SINGLE_JOB_SQS_URL = os.environ["SINGLE_JOB_SQS_URL"]

# define file conversion jobs here, currently only day-partitions are
# supported for live conversions
DEST_STORES = [
    {
        "dest_prefix": "version5/arrow/zst_lv22/day/",
        "file_format": "arrow",
        "compression": "zst",
        "compression_level": 22,
        "dest_store": "dataclient",
    },
]


def lambda_handler(event, context):
    """Prod listener function
    This lambda function receives new file and updated file events from the prod S3DB
    bucket + prefix. File conversion requests are generated based on these live events.
    """
    print(f"Event: {event}")

    for message in event["Records"]:
        sns_event = json.loads(message["body"])
        s3_event = json.loads(unquote(sns_event["Message"]))

        s3_key = s3_event["s3"]["object"]["key"]

        coll, ds, _ = s3_key.removeprefix(SOURCE_PREFIX).split("/", 2)

        for dest in DEST_STORES:
            partition_type = dest["partition_type"]
            # if it's a metadata file, just copy it directly
            if s3_key.endswith("METADATA.json") and partition_type == "dataclient":
                copy_metadata_file(coll, ds, dest["dest_prefix"])

            # trigger a conversion job
            else:
                SQS_CLIENT.send_message(
                    QueueUrl=SINGLE_JOB_SQS_URL,
                    MessageBody=json.dumps({"s3_key": s3_key, **dest}),
                )

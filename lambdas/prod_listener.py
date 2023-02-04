import json
import os
from urllib.parse import unquote

from loguru import logger

from lambdas.common import SOURCE_PREFIX, SQS_CLIENT, copy_metadata_file


SQS_BATCH_SIZE = 10

# define file conversion jobs here, currently only day-partitions are
# supported for live conversions
LIVE_STORES = [
    {
        "dest_store": "dataclient",
        "dest_prefix": "version5/arrow/zst_lv22/day/",
        "partition_size": "day",
        "file_format": "arrow",
        "compression": "zst",
        "compression_level": 22,
    },
    {
        "dest_store": "athena",
        "dest_prefix": "version5/athena/parquet/sz/day/",
        "partition_size": "day",
        "file_format": "parquet",
        "compression": "sz",
    },
]


def lambda_handler(event, context):
    """Prod listener function
    This lambda function receives new file and updated file events from the prod S3DB
    bucket + prefix. File conversion requests are generated based on these live events.
    """

    logger.info(event)

    for message in event["Records"]:
        sns_event = json.loads(message["body"])
        s3_event = json.loads(unquote(sns_event["Message"]))

        s3_key = s3_event["s3"]["object"]["key"]

        coll, ds, _ = s3_key.removeprefix(SOURCE_PREFIX).split("/", 2)

        for dest in LIVE_STORES:
            dest_store = dest["dest_store"]
            # if it's a metadata file, just copy it directly
            if s3_key.endswith("METADATA.json") and dest_store == "dataclient":
                logger.info(f"Copying over metadata file '{s3_key}'")
                copy_metadata_file(coll, ds, dest["dest_prefix"])

            # trigger a conversion job
            else:
                logger.info(f"Triggering Conversion job for '{s3_key}'")
                SQS_CLIENT.send_message(
                    QueueUrl=os.environ["SINGLE_JOB_SQS_URL"],
                    MessageBody=json.dumps({"s3_key": s3_key, **dest}),
                )

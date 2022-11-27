import json
import os
from datetime import timezone
from itertools import groupby

import boto3
from pydantic import BaseModel, validator

from lambdas.common import (
    SOURCE_PREFIX,
    batch_items,
    copy_metadata_file,
    extract_datetime,
    floor_dt,
    list_collections,
    list_datasets,
    list_keys,
)


SQS_CLIENT = boto3.client("sqs")
SQS_BATCH_SIZE = 10


def lambda_handler(event, context):
    print(f"Event: {event}")
    event = RequestGeneratorEvent(**event)

    for coll, dss in event.datasets.items():
        for ds in dss:
            copy_metadata_file(coll, ds, event.dest_prefix)

            if event.partition == "day":
                sqs_url = os.environ["SINGLE_JOB_SQS_URL"]

            elif event.partition == "month":
                sqs_url = os.environ["BATCH_JOB_SQS_URL"]

            else:
                raise Exception(f"Unknown period {event.partition}")

            items = generate_requests(
                coll, ds, event.compression, event.dest_prefix, event.partition
            )
            items = list(items)

            print(f"Submitting {len(items)} requests for '{coll}-{ds}'...")

            for batch in batch_items(items, SQS_BATCH_SIZE):
                SQS_CLIENT.send_message_batch(
                    QueueUrl=sqs_url,
                    Entries=[
                        {"Id": str(i), "MessageBody": json.dumps(k)}
                        for i, k in enumerate(batch)
                    ],
                )


def generate_requests(collection, dataset, compression, dest_prefix, period):
    prefix = os.path.join(SOURCE_PREFIX, collection, dataset, "")
    s3_keys = sorted(list_keys(collection, dataset))
    print(f"Found {len(s3_keys)} s3 keys for '{collection}-{dataset}'")
    gk_func = lambda key: floor_dt(extract_datetime(key), period)
    for gk, keys in groupby(s3_keys, key=gk_func):
        keys = [k.removeprefix(prefix) for k in keys]
        yield {
            "file_start": int(gk.replace(tzinfo=timezone.utc).timestamp()),
            "s3key_prefix": prefix,
            "s3key_suffixes": keys,
            "compression": compression,
            "dest_prefix": dest_prefix,
        }


class RequestGeneratorEvent(BaseModel):
    datasets: dict[str, list[str]]
    dest_prefix: str
    compression: str
    partition: str = "day"

    @validator("datasets")
    def datasets_exist(cls, v):
        invalid_colls = v.keys() - set(list_collections())
        if invalid_colls:
            raise Exception(f"Invalid collection(s): {invalid_colls}")

        for coll, ds in v.items():
            invalid_ds = set(ds) - set(list_datasets(coll))
            if invalid_ds:
                raise Exception(f"Invalid dataset(s): {invalid_ds}")

        return v

    @validator("dest_prefix")
    def valid_dest_prefix(cls, v):
        if not v.endswith("/") or v.startswith(SOURCE_PREFIX):
            raise ValueError(f"Invalid dest prefix: {v}")
        return v

import json
import os

import boto3
from pydantic import BaseModel, validator

from lambdas.common import (
    SOURCE_PREFIX,
    batch_items,
    copy_metadata_file,
    get_dataset_pkeys,
    list_collections,
    list_datasets,
    list_keys,
)


SQS_CLIENT = boto3.client("sqs")
SQS_BATCH_SIZE = 10
SQS_URL = os.environ["SQS_URL"]


def lambda_handler(event, context):
    print(f"Event: {event}")
    event = RequestGeneratorEvent(**event)

    for collection, datasets in event.datasets.items():
        for dataset in datasets:
            copy_metadata_file(collection, dataset, event.dest_prefix)

            pkeys = get_dataset_pkeys(collection, dataset)

            items = [
                single_key_request(s3_key, pkeys, event.compression, event.dest_prefix)
                for s3_key in list_keys(collection, dataset)
            ]
            print(f"Submitting {len(items)} requests for '{collection}-{dataset}'...")

            for batch in batch_items(items, SQS_BATCH_SIZE):
                SQS_CLIENT.send_message_batch(
                    QueueUrl=SQS_URL,
                    Entries=[
                        {"Id": str(i), "MessageBody": k} for i, k in enumerate(batch)
                    ],
                )


def single_key_request(s3_key, pkeys, compression, dest_prefix):
    packet = {
        "s3_key": s3_key,
        "pkeys": pkeys,
        "compression": compression,
        "dest_prefix": dest_prefix,
    }
    return json.dumps(packet)


class RequestGeneratorEvent(BaseModel):
    datasets: dict[str, list[str]]
    dest_prefix: str
    compression: str

    @validator("datasets", pre=True)
    def datasets_exist(cls, v):
        if v == "all":
            v = {c: list_datasets(c) for c in list_collections()}

        # validate collections
        elif isinstance(v, dict):
            invalid_colls = v.keys() - set(list_collections())
            if invalid_colls:
                raise Exception(f"Invalid collection(s): {invalid_colls}")

            for coll, ds in v.items():
                if ds == "all":
                    v[coll] = list_datasets(coll)

                # validate datasets
                elif isinstance(ds, list):
                    invalid_ds = set(ds) - set(list_datasets(coll))
                    if invalid_ds:
                        raise Exception(f"Invalid dataset(s): {invalid_ds}")

        return v

    @validator("dest_prefix")
    def valid_dest_prefix(cls, v):
        if not v.endswith("/") or v.startswith(SOURCE_PREFIX):
            raise ValueError(f"Invalid dest prefix: {v}")
        return v

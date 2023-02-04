import json
import os
from typing import Any, Optional

from loguru import logger
from pydantic import BaseModel, validator

from lambdas.common import (
    COMPRESSION,
    COMPRESSION_LEVELS,
    DEST_STORES,
    FILE_FORMATS,
    PARTITIONS,
    SOURCE_PREFIX,
    SQS_CLIENT,
    batch_items,
    copy_metadata_file,
    group_s3keys_by_partition,
    list_collections,
    list_datasets,
    list_keys,
)


SQS_BATCH_SIZE = 10


def lambda_handler(event, context):
    """Request Handler Function
    Receives backfill requests and generates file conversion jobs, before sending them
    off to the request handler function via SQS queues.
    """
    logger.info(event)
    event = RequestGeneratorEvent(**event)

    for coll, dss in event.datasets.items():
        for ds in dss:
            if event.partition_size in ("hour", "day"):
                sqs_url = os.environ["SINGLE_JOB_SQS_URL"]
            else:
                sqs_url = os.environ["BATCH_JOB_SQS_URL"]

            if event.dest_store == "dataclient":
                logger.info(f"Copying over metadata file for '{coll}.{ds}'")
                copy_metadata_file(coll, ds, event.dest_prefix)

            items = list(generate_requests(coll, ds, event))
            logger.info(f"Submitting {len(items)} requests for '{coll}-{ds}'...")

            for batch in batch_items(items, SQS_BATCH_SIZE):
                SQS_CLIENT.send_message_batch(
                    QueueUrl=sqs_url,
                    Entries=[
                        {"Id": str(i), "MessageBody": json.dumps(k)}
                        for i, k in enumerate(batch)
                    ],
                )


# main purpose of this is to validate user inputs
class RequestGeneratorEvent(BaseModel):
    datasets: dict[str, list[str]]
    dest_prefix: str
    compression: str
    compression_level: Optional[int] = None
    partition_size: str = "day"
    dest_store: str = "dataclient"
    file_format: str = "arrow"
    n_files: Optional[int] = None

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

    @validator("compression")
    def valid_compression(cls, v):
        if v not in COMPRESSION:
            raise ValueError(f"Invalid compression {v}")
        return v

    @validator("compression_level")
    def valid_compression_level(cls, v, values, **kwargs):
        codec = values["compression"]
        if v is not None and v not in COMPRESSION_LEVELS.get(codec, []):
            raise ValueError(f"Invalid compression level {v} for {codec}")
        return v

    @validator("partition_size")
    def valid_partition_size(cls, v):
        if v not in PARTITIONS.keys():
            raise ValueError(f"Invalid partition size {v}")
        return v

    @validator("dest_store")
    def valid_dest_store(cls, v):
        if v not in DEST_STORES:
            raise ValueError(f"Invalid dest_store {v}")
        return v

    @validator("file_format")
    def valid_file_format(cls, v):
        if v not in FILE_FORMATS:
            raise ValueError(f"Invalid file_format {v}")
        return v

    @validator("n_files")
    def valid_n_filest(cls, v):
        if v is not None and v <= 0:
            raise ValueError(f"Invalid n_files '{v}', must be positive.")
        return v


def generate_requests(collection: str, dataset: str, event: RequestGeneratorEvent):
    s3_keys = sorted(list_keys(collection, dataset))
    logger.info(f"Found {len(s3_keys)} s3 keys for '{collection}.{dataset}'")

    if event.n_files:
        s3_keys = s3_keys[-event.n_files :]
        logger.info(f"Selected latest {event.n_files} keys")

    prefix = os.path.join(SOURCE_PREFIX, collection, dataset, "")

    for gk, keys in group_s3keys_by_partition(s3_keys, event.partition_size):
        # remove the prefix to reduce payload size
        keys = [k.removeprefix(prefix) for k in keys]
        request: dict[str, Any] = {
            "s3key_prefix": prefix,
            "s3key_suffixes": keys,
            "compression": event.compression,
            "dest_prefix": event.dest_prefix,
            "partition_size": event.partition_size,
            "dest_store": event.dest_store,
            "file_format": event.file_format,
        }
        if event.compression_level is not None:
            request["compression_level"] = event.compression_level

        yield request

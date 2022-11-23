import gzip
import os
from itertools import islice

import boto3
import pyarrow as pa
from pyarrow import csv


# non-versioned bucket
SOURCE_BUCKET = "invenia-datafeeds-output"
SOURCE_PREFIX = "version5/aurora/gz/"

S3_CLIENT = boto3.client("s3")

EXT = {
    "zstd": "zst",
}


def list_source_keys(collection=None):
    s3_paginator = S3_CLIENT.get_paginator("list_objects_v2")
    prefix = os.path.join(SOURCE_PREFIX, collection) if collection else SOURCE_PREFIX

    yield from (
        i["Key"]
        for page in s3_paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=prefix)
        for i in page.get("Contents", ())
    )


def batch_items(itr, chunk_size):
    chunk = list(islice(itr, chunk_size))
    while chunk:
        yield chunk
        chunk = list(islice(itr, chunk_size))


def migrate_file(s3_key, dest_prefix, compression="zstd"):
    if s3_key.endswith(".csv.gz"):
        to_arrow(s3_key, dest_prefix, compression=compression)
    elif s3_key.endswith(".json"):  # metadata file
        copy_metadata_file(s3_key, dest_prefix)
    else:
        raise Exception(f"Unknown file {s3_key}")


def to_arrow(source_key, dest_prefix, compression):
    data = S3_CLIENT.get_object(Bucket=SOURCE_BUCKET, Key=source_key)["Body"]
    table = csv.read_csv(gzip.open(data))

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write(table)

    data = pa.compress(sink.getvalue(), codec="zstd", asbytes=True)
    dest_key = _gen_dest_key(source_key, dest_prefix, compression)
    S3_CLIENT.put_object(Bucket=SOURCE_BUCKET, Key=dest_key, Body=data)


def copy_metadata_file(source_key, dest_prefix):
    desk_key = os.path.join(dest_prefix, source_key.removeprefix(SOURCE_PREFIX))
    S3_CLIENT.copy_object(
        Bucket=SOURCE_BUCKET,
        Key=desk_key,
        CopySource={"Bucket": SOURCE_BUCKET, "Key": source_key},
    )


def _gen_dest_key(source_key, dest_prefix, compression):
    ext = EXT.get(compression, compression)
    filename = source_key.removeprefix(SOURCE_PREFIX).replace("csv.gz", f"arrow.{ext}")
    return os.path.join(dest_prefix, filename)

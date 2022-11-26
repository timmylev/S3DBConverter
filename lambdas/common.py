import gzip
import json
import os
from datetime import datetime, timezone
from itertools import islice

import boto3
import pyarrow as pa
from pyarrow import csv


# non-versioned bucket
SOURCE_BUCKET = "invenia-datafeeds-output"
SOURCE_PREFIX = "version5/aurora/gz/"

COMPRESSION = ["br", "bz2", "gz", "lz4", "zst", "sz"]
_PYARROW_ARG_TRANSLATION = {
    "br": "brotli",
    "gz": "gzip",
    "sz": "snappy",
    "zst": "zstd",
}

S3_CLIENT = boto3.client("s3")


def list_collections():
    coll_prefixes = _s3_list(SOURCE_BUCKET, SOURCE_PREFIX, dirs_only=True)
    return [c.split("/")[-2] for c in coll_prefixes]


def list_datasets(collection):
    prefix = os.path.join(SOURCE_PREFIX, collection, "")
    ds_prefixes = _s3_list(SOURCE_BUCKET, prefix, dirs_only=True)
    return [d.split("/")[-2] for d in ds_prefixes]


def list_keys(collection, dataset):
    prefix = os.path.join(SOURCE_PREFIX, collection, dataset, "")
    yield from (i for i in _s3_list(SOURCE_BUCKET, prefix) if i.endswith(".csv.gz"))


def gen_metadata_key(collection, dataset):
    return os.path.join(SOURCE_PREFIX, collection, dataset, "METADATA.json")


def get_dataset_pkeys(collection, dataset):
    key = gen_metadata_key(collection, dataset)
    meta = json.load(S3_CLIENT.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"])
    return meta["superkey"]


def extract_datetime(s3_key):
    filename = s3_key.rsplit("/", 1)[-1]
    ts = int(filename.split(".")[0])
    return datetime.fromtimestamp(ts, timezone.utc)


def copy_metadata_file(collection, dataset, dest_prefix):
    key = gen_metadata_key(collection, dataset)
    desk_key = os.path.join(dest_prefix, key.removeprefix(SOURCE_PREFIX))
    S3_CLIENT.copy_object(
        Bucket=SOURCE_BUCKET,
        Key=desk_key,
        CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
    )


def migrate_to_arrow(source_keys, file_start, dest_prefix, compression):
    table = _convert_to_arrow(source_keys)

    sink = pa.BufferOutputStream()
    codec = _PYARROW_ARG_TRANSLATION.get(compression, compression)
    cfg = pa.ipc.IpcWriteOptions(compression=codec)

    with pa.ipc.new_stream(sink, table.schema, options=cfg) as writer:
        writer.write(table)

    coll, ds = source_keys[0].removeprefix(SOURCE_PREFIX).split("/")[:2]
    dest_key = _gen_dest_key(file_start, coll, ds, dest_prefix, compression)
    S3_CLIENT.put_object(Bucket=SOURCE_BUCKET, Key=dest_key, Body=sink.getvalue())


def _convert_to_arrow(source_keys):
    tables = (
        csv.read_csv(
            gzip.open(S3_CLIENT.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"])
        )
        for key in source_keys
    )

    table = pa.concat_tables(tables)

    not_nulls = [k for k in table.column_names if table.column(k).null_count == 0]
    schema = pa.schema([f.with_nullable(f.name not in not_nulls) for f in table.schema])

    return table.cast(schema)


def batch_items(itr, chunk_size):
    itr = iter(itr)
    chunk = list(islice(itr, chunk_size))
    while chunk:
        yield chunk
        chunk = list(islice(itr, chunk_size))


def _s3_list(bucket, prefix, dirs_only=False):
    pg = S3_CLIENT.get_paginator("list_objects_v2")
    arg = {"Bucket": bucket, "Prefix": prefix}
    if dirs_only:
        arg["Delimiter"] = "/"
        yield from (
            i["Prefix"] for p in pg.paginate(**arg) for i in p.get("CommonPrefixes", ())
        )
    else:
        yield from (i["Key"] for p in pg.paginate(**arg) for i in p.get("Contents", ()))


def _gen_dest_key(file_start, coll, ds, dest_prefix, compression):
    year = datetime.fromtimestamp(file_start, timezone.utc).year
    filename = f"year={year}/{file_start}.arrow.{compression}"
    return os.path.join(dest_prefix, coll, ds, filename)


def floor_dt(dt, period):
    if period == "day":
        return datetime(dt.year, dt.month, dt.day)
    elif period == "month":
        return datetime(dt.year, dt.month, 1)
    elif period == "year":
        return datetime(dt.year, 1, 1)
    else:
        raise Exception(f"invalid period: {period}")


# def show_memory(text):
#     import psutil
#     process = psutil.Process(os.getpid())
#     mb = process.memory_info().rss / 1_000_000
#     print(f"{text}: {mb}MB")

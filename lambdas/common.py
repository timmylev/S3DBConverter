import concurrent.futures
import gzip
import io
import json
import os
from datetime import datetime, timezone
from itertools import islice

import boto3
import psutil
import pyarrow as pa
from boto3.s3.transfer import TransferConfig
from pyarrow import csv


# non-versioned bucket
SOURCE_BUCKET = "invenia-datafeeds-output"
SOURCE_PREFIX = "version5/aurora/gz/"

COMPRESSION = ["br", "gz", "lz4", "zst", "sz"]
_PYARROW_ARG_TRANSLATION = {
    "br": "brotli",
    "gz": "gzip",
    "sz": "snappy",
    "zst": "zstd",
}

S3_CLIENT = boto3.client("s3")
# Default multi-part config:
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#module-boto3.s3.inject
S3_CONFIG = TransferConfig()


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
    stream = _convert_to_arrow(source_keys, compression)

    coll, ds = source_keys[0].removeprefix(SOURCE_PREFIX).split("/")[:2]
    dest_key = _gen_dest_key(file_start, coll, ds, dest_prefix, compression)

    _s3_multi_p_upload(SOURCE_BUCKET, dest_key, stream)

    # S3_CLIENT.put_object(Bucket=SOURCE_BUCKET, Key=dest_key, Body=stream)


def _convert_to_arrow(source_keys, compression):
    def download(i, k):
        data = S3_CLIENT.get_object(Bucket=SOURCE_BUCKET, Key=k)["Body"]
        table = csv.read_csv(gzip.open(data))
        show_memory(f"loaded table {i}")
        return table

    # for large datasets such as caiso prices, aws lambda hits max memory (10gb)
    # at around 250 files/days, so we probably can't use lambda to batch yearly files.
    with concurrent.futures.ThreadPoolExecutor(10) as executor:
        idx = range(1, len(source_keys) + 1)
        tables = [r for r in executor.map(download, idx, source_keys)]

    table = pa.concat_tables(tables, promote=True)
    show_memory("loaded all tables")

    not_nulls = [k for k in table.column_names if table.column(k).null_count == 0]
    schema = pa.schema([f.with_nullable(f.name not in not_nulls) for f in table.schema])
    table.cast(schema)

    cdec = _PYARROW_ARG_TRANSLATION.get(compression, compression)
    sink = io.BytesIO()

    # on-the-fly compression is way more memory efficient, but only possible for
    # zst and lz4
    cfg = pa.ipc.IpcWriteOptions(compression=cdec) if cdec in ("zstd", "lz4") else None
    with pa.ipc.new_stream(sink, table.schema, options=cfg) as writer:
        writer.write(table)

    sink.seek(0)

    # for non zst/lz4 compressions, uses much higher memory overall
    if cfg is None:
        data = pa.compress(sink.getvalue(), codec=cdec, asbytes=True)
        sink = io.BytesIO(data)

    show_memory("compressed arrow stream")

    return sink  # can be a stream or bytes


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


def _s3_multi_p_upload(bucket, s3_key, file_obj):
    extra_args = {"ACL": "bucket-owner-full-control"}
    S3_CLIENT.upload_fileobj(
        Bucket=bucket,
        Key=s3_key,
        Fileobj=file_obj,
        Config=S3_CONFIG,
        ExtraArgs=extra_args,
    )


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


def show_memory(text):
    process = psutil.Process(os.getpid())
    mb = process.memory_info().rss / 1_000_000
    print(f"{text}: {mb}MB")

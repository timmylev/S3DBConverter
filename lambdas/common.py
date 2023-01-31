import concurrent.futures
import gzip
import io
import json
import os
from datetime import datetime, timezone
from itertools import groupby, islice

import boto3
import psutil
import pyarrow as pa
from boto3.s3.transfer import TransferConfig
from pyarrow import csv


# non-versioned bucket
SOURCE_BUCKET = "invenia-datafeeds-output"
SOURCE_PREFIX = "version5/aurora/gz/"

COMPRESSION = ["br", "gz", "lz4", "zst", "sz"]
COMPRESSION_LEVELS = {
    # Higher values are slower and have higher compression ratios.
    "zst": range(-131072, 22 + 1),  # min: -131072, max: 22, default: 1
    "br": range(0, 11 + 1),  # min: 0, max: 11, pyarrow default = 8
    # Higher levels use more RAM but are faster and have higher compression ratios.
    "gz": range(1, 9 + 1),  # min: 1, max: 9, pyarrow default = 9
}
COMPRESSION_LEVELS_DEFAULTS = {
    "zst": 22,
    "br": 9,
    "gz": 9,
}
_PYARROW_ARG_TRANSLATION = {
    "br": "brotli",
    "gz": "gzip",
    "sz": "snappy",
    "zst": "zstd",
}

DEST_STORES = ["athena", "dataclient"]
FILE_FORMATS = ["arrow", "parquet"]
PARTITION_SIZES = ["hour", "day", "month", "year"]

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


def migrate_data(
    source_keys,
    dest_prefix,
    dest_store,
    partition_size,
    file_format,
    compression,
    level=None,
):
    for ts, coll, ds, table in load_as_partitions(source_keys, partition_size):
        if dest_store == "athena":
            if file_format != "parquet":
                raise Exception(
                    f"Only parquet is supported for Athena, found {file_format}"
                )
            data = _compress_to_bytes(table, compression, to_parquet=True)
            key = _gen_athena_key(ts, coll, ds, dest_prefix)
        else:
            to_parquet = file_format == "parquet"
            data = _compress_to_bytes(
                table, compression, level=level, to_parquet=to_parquet
            )
            key = _gen_s3db_key(ts, coll, ds, dest_prefix, file_format, compression)

        _s3_multi_p_upload(SOURCE_BUCKET, key, io.BytesIO(data))


def load_as_partitions(source_keys, partition_size):
    for gk, s3keys in group_s3keys_by_partition(source_keys, partition_size):
        coll, ds, file_start = gk
        table = _get_arrow_table(s3keys)

        # for hourly partitions, we'll have to further split the file/table
        if partition_size == "hour":
            # TODO: partition table by hour
            yield file_start, coll, ds, table
        else:
            yield file_start, coll, ds, table


def _get_arrow_table(source_keys):
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

    not_nulls = [k for k in table.column_names if table.column(k).null_count == 0]
    schema = pa.schema([f.with_nullable(f.name not in not_nulls) for f in table.schema])
    table = table.cast(schema)
    show_memory("loaded all tables")

    return table


def _compress_to_bytes(table, compression, level=None, to_parquet=False):
    sink = io.BytesIO()
    codec_key = _PYARROW_ARG_TRANSLATION.get(compression, compression)

    if to_parquet:
        pa.parquet.write_table(table, sink, compression=codec_key)
        data = sink.getvalue()

    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write(table)

        show_memory("written table to byte stream")
        codec = pa.Codec(codec_key, compression_level=level)
        data = codec.compress(sink.getvalue(), asbytes=True)

    sink.close()
    show_memory("compressed byte stream")

    return data


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


def _gen_s3db_key(file_start, coll, ds, dest_prefix, file_format, compression):
    year = datetime.fromtimestamp(file_start, timezone.utc).year
    filename = f"year={year}/{file_start}.{file_format}.{compression}"
    return os.path.join(dest_prefix, coll, ds, filename)


def _gen_athena_key(file_start, coll, ds, dest_prefix):
    # https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    dt_fmt = "%d-%m-%Y-%H-%M-%S"
    partition_val = datetime.fromtimestamp(file_start, timezone.utc).strftime(dt_fmt)
    filename = f"dt={partition_val}/{file_start}.parquet"
    return os.path.join(dest_prefix, coll, ds, filename)


def group_s3keys_by_partition(source_keys, partition_size):
    def gk_func(key):
        coll, ds = key.removeprefix(SOURCE_PREFIX).split("/")[:2]
        file_start = floor_dt(extract_datetime(key), partition_size)
        return coll, ds, file_start

    source_keys.sort()
    for gk, keys in groupby(source_keys, key=gk_func):
        yield gk, keys


def floor_dt(dt, period):
    if period == "hour":
        return datetime(dt.year, dt.month, dt.day, dt.hour)
    elif period == "day":
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

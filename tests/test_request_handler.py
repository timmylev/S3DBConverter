import io
import json
import re
from datetime import datetime, timezone

import boto3
import pandas as pd
import pytest

from lambdas.common import (
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    _s3_list,
    extract_datetime,
    floor_dt,
    refresh_clients,
)
from lambdas.request_generator import RequestGeneratorEvent, generate_requests
from lambdas.request_handler import lambda_handler
from tests.aws_setup import mock_start, mock_stop, setup_resources


@pytest.fixture()
def patched_bucket():
    mock_start()
    refresh_clients()
    yield setup_resources()
    mock_stop()


# helper function to generate request handler events
def generate_events(coll, ds, partition, fmt, dest_store, compression="zst"):
    payload = {
        "datasets": {coll: [ds]},
        "dest_prefix": f"test/{dest_store}/{partition}/{fmt}/{compression}/",
        "compression": compression,
        "partition_size": partition,
        "dest_store": dest_store,
        "file_format": fmt,
    }
    job = RequestGeneratorEvent(**payload)
    return list(generate_requests(coll, ds, job))


is_dataclient = lambda key: re.search(r"year=\d{4}")  # type: ignore
is_athena = lambda key: re.search(r"dt=\d{4}")  # type: ignore


def test_handler_for_dataclient(patched_bucket):
    coll, ds, dest_store, fmt = "pjm", "dayahead_price", "dataclient", "arrow"
    reg = re.compile(r"year=\d{4}.*\.arrow\.zst")

    # test daily partition
    events = generate_events(coll, ds, "day", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    # source files are partitioned by day as well
    assert len(dest_files) == len(source_files)
    # check that dataclient-styled path is generated with the correct extension
    assert all(reg.search(key) for key in dest_files)

    # test hourly partition
    events = generate_events(coll, ds, "hour", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    # there be 24x dest files because source files are partitioned by day
    assert len(dest_files) == len(source_files) * 24
    # check that dataclient-styled path is generated with the correct extension
    assert all(reg.search(key) for key in dest_files)

    # test monthly partition
    events = generate_events(coll, ds, "month", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    assert len(dest_files) == 3  # data -> 2020-1-1 to 2020-3-2, so 3 months total
    # check that dataclient-styled path is generated with the correct extension
    assert all(reg.search(key) for key in dest_files)

    # test yearly partition
    events = generate_events(coll, ds, "year", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    assert len(dest_files) == 1
    # check that dataclient-styled path is generated with the correct extension
    assert all(reg.search(key) for key in dest_files)


def test_handler_for_athena(patched_bucket):
    coll, ds, dest_store, fmt = "pjm", "dayahead_price", "athena", "parquet"
    reg = re.compile(r"dt=\d{2}-\d{2}-\d{4}-\d{2}-\d{2}-\d{2}.*\.parquet")
    client = boto3.client("s3")

    # test daily partition
    events = generate_events(coll, ds, "day", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    # source files are partitioned by day as well
    assert len(dest_files) == len(source_files)
    # check that it is a valid parquet file containing the correct data
    part_key = lambda ts: floor_dt(datetime.fromtimestamp(ts), "day").replace(
        tzinfo=timezone.utc
    )
    for key in dest_files:
        assert reg.search(key)
        data = client.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
        pk = set(map(part_key, df.target_start))
        assert len(pk) == 1
        assert pk.pop() == extract_datetime(key)

    # test hourly partition
    events = generate_events(coll, ds, "hour", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    # there be 24x dest files because source files are partitioned by day
    assert len(dest_files) == len(source_files) * 24
    # check that it is a valid parquet file containing the correct data
    part_key = lambda ts: floor_dt(datetime.fromtimestamp(ts), "hour").replace(
        tzinfo=timezone.utc
    )
    for key in dest_files:
        assert reg.search(key)
        data = client.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
        pk = set(map(part_key, df.target_start))
        assert len(pk) == 1
        assert pk.pop() == extract_datetime(key)

    # test monthly partition
    events = generate_events(coll, ds, "month", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    assert len(dest_files) == 3  # data -> 2020-1-1 to 2020-3-2, so 3 months total
    # check that it is a valid parquet file containing the correct data
    part_key = lambda ts: floor_dt(datetime.fromtimestamp(ts), "month").replace(
        tzinfo=timezone.utc
    )
    for key in dest_files:
        assert reg.search(key)
        data = client.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
        pk = set(map(part_key, df.target_start))
        assert len(pk) == 1
        assert pk.pop() == extract_datetime(key)

    # test yearly partition
    events = generate_events(coll, ds, "year", fmt, dest_store)
    for event in events:
        lambda_handler({"Records": [{"body": json.dumps(event)}]}, None)
    source_files = list(_s3_list(SOURCE_BUCKET, f"{SOURCE_PREFIX}{coll}/{ds}"))
    dest_files = list(_s3_list(SOURCE_BUCKET, f"{events[0]['dest_prefix']}{coll}/{ds}"))
    assert len(dest_files) == 1
    # check that it is a valid parquet file containing the correct data
    part_key = lambda ts: floor_dt(datetime.fromtimestamp(ts), "year").replace(
        tzinfo=timezone.utc
    )
    for key in dest_files:
        assert reg.search(key)
        data = client.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
        pk = set(map(part_key, df.target_start))
        assert len(pk) == 1
        assert pk.pop() == extract_datetime(key)

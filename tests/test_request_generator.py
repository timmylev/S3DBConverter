import pytest

from lambdas.common import (
    SOURCE_PREFIX,
    extract_datetime,
    floor_dt,
    list_keys,
    refresh_clients,
)
from lambdas.request_generator import RequestGeneratorEvent, generate_requests
from tests.aws_setup import mock_start, mock_stop, setup_resources


@pytest.fixture()
def patched_bucket():
    mock_start()
    refresh_clients()
    yield setup_resources()
    mock_stop()


def test_request_generator_event(patched_bucket):
    # test defaults
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
    }
    event = RequestGeneratorEvent(**request)
    assert event.compression_level is None
    assert event.partition_size == "day"
    assert event.dest_store == "dataclient"
    assert event.file_format == "arrow"

    # test custom values
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
        "compression_level": 22,
        "partition_size": "hour",
        "dest_store": "athena",
        "file_format": "parquet",
    }
    event = RequestGeneratorEvent(**request)
    assert event.compression_level == 22
    assert event.partition_size == "hour"
    assert event.dest_store == "athena"
    assert event.file_format == "parquet"

    # test non-existent collection
    request = {
        "datasets": {"miso": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
    }
    with pytest.raises(Exception, match="Invalid collection"):
        event = RequestGeneratorEvent(**request)

    # test non-existent dataset
    request = {
        "datasets": {"pjm": ["realtime_load"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
    }
    with pytest.raises(Exception, match="Invalid dataset"):
        event = RequestGeneratorEvent(**request)

    # test invalid dest prefix, overlaps with prod
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": f"{SOURCE_PREFIX}/test",
        "compression": "zst",
    }
    with pytest.raises(Exception, match="Invalid dest prefix"):
        event = RequestGeneratorEvent(**request)

    # test invalid compression
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "bzip2",
    }
    with pytest.raises(Exception, match="Invalid compression"):
        event = RequestGeneratorEvent(**request)

    # test invalid compression level
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
        "compression_level": 23,  # max for zst if 22
    }
    with pytest.raises(Exception, match="Invalid compression level"):
        event = RequestGeneratorEvent(**request)

    # test partition size
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
        "partition_size": "7days",
    }
    with pytest.raises(Exception, match="Invalid partition size"):
        event = RequestGeneratorEvent(**request)

    # test invalid compression
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
        "dest_store": "aurora",
    }
    with pytest.raises(Exception, match="Invalid dest_store"):
        event = RequestGeneratorEvent(**request)

    # test invalid compression
    request = {
        "datasets": {"pjm": ["realtime_price"]},
        "dest_prefix": "version5/test/",
        "compression": "zst",
        "file_format": "json",
    }
    with pytest.raises(Exception, match="Invalid file_format"):
        event = RequestGeneratorEvent(**request)


def test_generate_requests(patched_bucket):
    coll, ds = "pjm", "realtime_price"
    attrs = {"datasets": {coll: [ds]}, "dest_prefix": "test/", "compression": "zst"}

    # Test daily partition
    event = RequestGeneratorEvent(partition_size="day", compression_level=22, **attrs)
    requests = list(generate_requests(coll, ds, event))
    # prod is also partitioned by day, so num requests should be the same
    assert len(requests) == len(list(list_keys(coll, ds)))
    assert all(len(r["s3key_suffixes"]) == 1 for r in requests)
    assert all(r["s3key_prefix"] == f"{SOURCE_PREFIX}{coll}/{ds}/" for r in requests)
    assert all(r["compression_level"] == 22 for r in requests)

    # Test hourly partition
    # no different from daily partition becuase source files are daily,
    # data partitioning happens on handler, not request generation.
    event = RequestGeneratorEvent(partition_size="hour", **attrs)
    requests = list(generate_requests(coll, ds, event))
    # prod is also partitioned by day, so num requests should be the same
    assert len(requests) == len(list(list_keys(coll, ds)))
    assert all(len(r["s3key_suffixes"]) == 1 for r in requests)
    assert all(r["s3key_prefix"] == f"{SOURCE_PREFIX}{coll}/{ds}/" for r in requests)
    # no compression level was specified this time
    assert all("compression_level" not in r for r in requests)

    # Test monthly partition
    event = RequestGeneratorEvent(partition_size="month", **attrs)
    requests = list(generate_requests(coll, ds, event))
    assert len(requests) == 18  # test data -> 2020-1-1 to 2021-6-15, so 18 months total
    key_func = lambda key: floor_dt(extract_datetime(key), "month")
    for r in requests:
        assert r["s3key_prefix"] == f"{SOURCE_PREFIX}{coll}/{ds}/"
        # check that all keys belong to the same partition
        assert len(set([key_func(k) for k in r["s3key_suffixes"]])) == 1

    # Test yearly partition
    event = RequestGeneratorEvent(partition_size="year", **attrs)
    requests = list(generate_requests(coll, ds, event))
    assert len(requests) == 2  # test data -> 2020-1-1 to 2021-6-15, so 2 years total
    key_func = lambda key: floor_dt(extract_datetime(key), "year")
    for r in requests:
        assert r["s3key_prefix"] == f"{SOURCE_PREFIX}{coll}/{ds}/"
        # check that all keys belong to the same partition
        assert len(set([key_func(k) for k in r["s3key_suffixes"]])) == 1

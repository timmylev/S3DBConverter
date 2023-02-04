import gzip
import json
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
from moto import mock_s3

from lambdas.common import SOURCE_BUCKET, SOURCE_PREFIX, gen_metadata_key


s3 = mock_s3()


def mock_start():
    s3.start()


def mock_stop():
    s3.stop()


def setup_resources():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=SOURCE_BUCKET)
    insert_test_data("pjm", "realtime_price")
    insert_test_data("pjm", "dayahead_price", days=70)


def insert_test_data(coll, ds, days=520):
    s3_client = boto3.client("s3")
    header = "target_start,target_end,node_id,lmp"
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    one_hour = int(timedelta(hours=1).total_seconds())

    lim = start + timedelta(days=days)
    while start <= lim:
        year_partition = f"year={start.year}"
        filename = f"{int(start.timestamp())}.csv.gz"
        s3_key = os.path.join(SOURCE_PREFIX, coll, ds, year_partition, filename)

        lines = [header]
        for h in range(24):  # 24 hr / day
            start_ts = int(start.timestamp())
            lines.append(f"{start_ts},{start_ts + one_hour},{h},8.9")
            start += timedelta(hours=1)

        stream = BytesIO()
        with gzip.GzipFile(fileobj=stream, mode="w") as f:
            csv_data = "\n".join(lines)
            f.write(csv_data.encode())

        s3_client.put_object(Bucket=SOURCE_BUCKET, Key=s3_key, Body=stream.getvalue())

    metadata = {
        "type_map": {
            "target_start": "int",
            "target_end": "int",
            "node_id": "int",
            "lmp": "float",
        },
        "superkey": ["target_start", "target_end", "node_id"],
        "value_key": ["lmp"],
    }
    data = json.dumps(metadata).encode()
    key = gen_metadata_key(coll, ds)
    s3_client.put_object(Bucket=SOURCE_BUCKET, Key=key, Body=data)

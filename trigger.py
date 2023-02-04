import json
import os
import sys
from enum import Enum

import boto3
from pyfiglet import Figlet
from PyInquirer import Separator, prompt
from termcolor import cprint

from deploy import STACK_NAME
from lambdas.common import (
    COMPRESSION,
    COMPRESSION_LEVELS,
    COMPRESSION_LEVELS_DEFAULTS,
    DEST_STORES,
    FILE_FORMATS,
    PARTITIONS,
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    gen_partition_key,
    get_dataset_type_map,
    list_collections,
    list_datasets,
)
from lambdas.prod_listener import LIVE_STORES


PROD_ACCOUNT = 516256908252
# ATHENA_WORKGROUP = "s3db_admin"
# DBS = ["caiso", "ercot", "iso_ne", "miso", "nyiso", "pjm", "spp", "weather"]

# Glue Catalog/Table and Athena Configs
DATA_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
DATA_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
SER_DER_LIB = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
DATA_LOCATION = "s3://invenia-datafeeds-output/version5/athena/parquet/sz/day/"
PARQUET_COMPRESSION = "SNAPPY"
PARTITION_SIZE = "day"
PARTITION_KEY = gen_partition_key(PARTITION_SIZE)
PARTITION_TYPE = PARTITIONS[PARTITION_SIZE]["type"]
# This type is different from the PartitionKeyType above, it is only
# used during partition projection, we want it to be always a 'date'
PARTITION_PROJECTION_TYPE = "date"
PARTITION_PROJECTION_FORMAT = PARTITIONS[PARTITION_SIZE]["projection_format"]
PARTITION_PROJECTION_UNIT = PARTITIONS[PARTITION_SIZE]["unit"]
PARTITION_PROJECTION_INTERVAL = "1"
PARTITION_PROJECTION_END = "NOW+1WEEKS"


class GlueOptions(str, Enum):
    CREATE = "Create tables for new datasets"
    DELETE = "Delete existing tables"
    REPAIR = "Validate and Repair table schemas"


class BackfillRange(str, Enum):
    ALL = "All files"
    LATEST_N = "Latest N files"


class Options(str, Enum):
    BACKFILLS = "Trigger S3DB Conversions"
    MANAGE_ATHENA = "Manage AthenaDB"
    EXIT = "Exit"


def main():
    print_welcome()

    api = API(prompt_text("Stack Name:", default=STACK_NAME))

    while True:
        action = prompt_options(
            "What would you like to do:", [i.value for i in Options]
        )

        if action == Options.BACKFILLS:
            prompt_backfills(api)

        elif action == Options.MANAGE_ATHENA:
            prompt_athena_manager(api)

        elif action == Options.EXIT:
            print("Terminating...")
            sys.exit(0)


class API:
    def __init__(self, stack_name):
        self.stack_name = stack_name
        self._stack_outputs = None

        self.aws_sesh = boto3.session.Session()

        resp = self.aws_sesh.client("sts").get_caller_identity()
        if int(resp["Account"]) != PROD_ACCOUNT:
            raise Exception("Please assume prod account role.")

        self.cfn = self.aws_sesh.client("cloudformation")
        self.lmb = self.aws_sesh.client("lambda")
        self.athena = self.aws_sesh.client("athena")
        self.glue = self.aws_sesh.client("glue")

    @property
    def stack_outputs(self):
        if self._stack_outputs is None:
            resp = self.cfn.describe_stacks(StackName=self.stack_name)
            outputs = resp["Stacks"][0]["Outputs"]
            self._stack_outputs = {el["OutputKey"]: el["OutputValue"] for el in outputs}

        return self._stack_outputs

    def trigger_lambda(
        self,
        dest_store,
        dest_prefix,
        file_format,
        compression,
        partition_size,
        datasets,
        compression_level=None,
        n_files=None,
    ):
        event = {
            "dest_store": dest_store,
            "dest_prefix": dest_prefix,
            "file_format": file_format,
            "compression": compression,
            "partition_size": partition_size,
            "datasets": datasets,
        }
        if compression_level:
            event["compression_level"] = compression_level

        if n_files:
            event["n_files"] = n_files

        self.lmb.invoke(
            FunctionName=self.stack_outputs["RequestGeneratorFunctionName"],
            InvocationType="Event",
            Payload=json.dumps(event),
        )

    def check_glue_catalog(self):
        s3db_colls = set(list_collections())
        glue_dbs = set([el["Name"] for el in self.glue.get_databases()["DatabaseList"]])

        missing_dbs = s3db_colls - glue_dbs
        if missing_dbs:
            print(f"  Detected {len(missing_dbs)} missing database(s)")
            for db in missing_dbs:
                print(f"    Creating database '{db}'...")
                self.glue.create_database(DatabaseInput={"Name": db})

        available_tables = {db: list_datasets(db) for db in s3db_colls}
        created_tables = {db: list(self.list_glue_tables(db)) for db in s3db_colls}
        missing_tables = {
            db: set(available_tables[db]) - set([t["Name"] for t in created_tables[db]])
            for db in s3db_colls
        }
        return created_tables, missing_tables

    def list_glue_tables(self, db):
        paginator = self.glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=db):
            yield from page["TableList"]

    def create_glue_table(self, db, table, update=False):
        operation = self.glue.update_table if update else self.glue.create_table
        # TODO: find the start date per dataset dynamically
        pp_range = f"2010-01-01,{PARTITION_PROJECTION_END}"
        operation(
            DatabaseName=db,
            TableInput={
                "Name": table,
                "StorageDescriptor": {
                    "Columns": self.get_glue_type_map_from_s3db(db, table),
                    "Location": f"{DATA_LOCATION}{db}/{table}",
                    "InputFormat": DATA_INPUT_FORMAT,
                    "OutputFormat": DATA_OUTPUT_FORMAT,
                    # None because we already use parquet columnar compression, which
                    # is specified in the table properties, not here.
                    "Compressed": False,
                    "SerdeInfo": {"SerializationLibrary": SER_DER_LIB},
                },
                "PartitionKeys": [{"Name": PARTITION_KEY, "Type": PARTITION_TYPE}],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    f"projection.{PARTITION_KEY}.type": PARTITION_PROJECTION_TYPE,
                    f"projection.{PARTITION_KEY}.interval.unit": PARTITION_PROJECTION_UNIT,  # noqa 501
                    f"projection.{PARTITION_KEY}.interval": PARTITION_PROJECTION_INTERVAL,  # noqa 501
                    f"projection.{PARTITION_KEY}.range": pp_range,
                    f"projection.{PARTITION_KEY}.format": PARTITION_PROJECTION_FORMAT,
                    "projection.enabled": "TRUE",
                    "parquet.compression": PARQUET_COMPRESSION,
                },
            },
        )

    def delete_glue_table(self, db, table):
        self.glue.delete_table(DatabaseName=db, Name=table)

    def get_glue_type_map_from_s3db(self, db, table):
        s3db_types = get_dataset_type_map(db, table)
        overrides = {
            "target_start": "bigint",
            "target_end": "bigint",
            "release_date": "bigint",
            "target_bounds": "tinyint",
        }
        conversions = {
            "str": "string",
            "float": "double",
            "bool": "tinyint",  # datafeeds stored these as ints
            "timedelta": "double",  # datafeeds stored these as floats
            "datetime": "bigint",
            # TODO: check converted parquet type
            "list": "string",  # datasoup.ercot_da_energy_bids
        }
        return [
            {"Name": k, "Type": overrides.get(k, conversions.get(v, v))}
            for k, v in s3db_types.items()
        ]


def print_welcome():
    cprint(Figlet(font="big").renderText("S3DB   CLI"), "blue")
    print("S3DB Source Location:")
    cprint(f"  s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}", "green")
    print("Live Conversions:")
    for el in LIVE_STORES:
        uri = f"s3://{SOURCE_BUCKET}/{el['dest_prefix']}"
        cprint(f"  ({el['dest_store']}) {uri}", "green")
    print("AthenaDB Source:")
    cprint(f"  {DATA_LOCATION}", "green")
    print("\nWelcome to S3DB CLI! Just follow the prompts.\n")


def prompt_backfills(api):
    dest_store = prompt_options("Select destination type:", DEST_STORES)

    if dest_store == "dataclient":
        file_fmt = prompt_options("Select file format:", FILE_FORMATS)
    else:
        file_fmt = "parquet"

    compression = prompt_options("Select compression:", COMPRESSION)

    if dest_store == "dataclient" and compression in COMPRESSION_LEVELS:
        levels = COMPRESSION_LEVELS[compression]
        validate = lambda x: int(x) in levels
        msg = f"Select compression level ({min(levels)} to {max(levels)}):"
        default = str(COMPRESSION_LEVELS_DEFAULTS[compression])
        compression_level = int(prompt_text(msg, default, validate))
        codec_str = f"{compression}_lv{compression_level}"
    else:
        compression_level = None
        codec_str = compression

    partition = prompt_options("Select dest partition size:", list(PARTITIONS.keys()))

    if partition == "year":
        print(
            "WARNING: Partitioning by year will fail for very large datasets such as "
            "CAISO Price Data due to AWS Lambda hitting max memory (10GB)."
        )

    dest_prefix = prompt_text(
        "Specify destination s3 prefix:",
        default="/".join(["version5", dest_store, file_fmt, codec_str, partition, ""]),
    )
    dest_prefix = os.path.join(dest_prefix, "")

    targets = {}
    collections = list_collections()
    options = [
        "DONE",
        "CANCEL",
        Separator("======== collections ========"),
        "ALL",
        *collections,
    ]

    while True:
        coll = prompt_options("Select collection:", options)
        if coll == "DONE":
            break
        elif coll == "CANCEL":
            targets = {}
            break
        elif coll == "ALL":
            targets = {c: list_datasets(c) for c in collections}
            break
        else:
            ds = prompt_checkbox("Select dataset(s):", list_datasets(coll))
            if ds:
                targets[coll] = sorted(set([*ds, *targets.get(coll, [])]))

    if not targets:
        print("No datasets selected...")

    else:
        n_files = prompt_options("Select file range:", [e.value for e in BackfillRange])
        if n_files == BackfillRange.LATEST_N:
            n_files = prompt_text("Specify number of files:")
        else:
            n_files = None

        num_datasets = sum([len(v) for v in targets.values()])
        msg = f"Backfilling {num_datasets} datasets. Proceed?"
        if prompt_confirmation(msg):
            print("Trigerring Request Generator...", end="", flush=True)

            for coll, ds in targets.items():
                if ds:
                    api.trigger_lambda(
                        dest_store,
                        dest_prefix,
                        file_fmt,
                        compression,
                        partition,
                        {coll: ds},
                        compression_level=compression_level,
                        n_files=n_files,
                    )

            print(" Done")
        else:
            print("Cancelling...")


def prompt_athena_manager(api):
    options = [GlueOptions.REPAIR, GlueOptions.DELETE]

    print("Checking Athena status...")
    created_tables, missing_tables = api.check_glue_catalog()
    has_missing = any([tbs for tbs in missing_tables.values()])

    if has_missing:
        options = [GlueOptions.CREATE, *options]
        print("  New datasets detected:")
        for db, tables in missing_tables.items():
            if tables:
                print(f"    - {db} ({len(tables)} datasets): {sorted(tables)}")

    print("Existing Databases and Tables:")
    for db, tables in created_tables.items():
        if tables:
            print(f"  {db}: {len(tables)} tables")

    opts = [opt.value for opt in options]
    selected = prompt_options("What would you like to do?", opts)

    if selected == GlueOptions.CREATE:
        tables = sorted([f"{db}.{t}" for db, ts in missing_tables.items() for t in ts])
        to_create = prompt_checkbox("Select tables:", tables)
        for el in to_create:
            db, tbl = el.split(".")
            print(f"Creating Glue Table '{db}.{el}'...")
            api.create_glue_table(db, tbl)

    elif selected == GlueOptions.DELETE:
        tables = sorted(
            [f"{db}.{t['Name']}" for db, tbls in created_tables.items() for t in tbls]
        )
        to_delete = prompt_checkbox("Select tables:", tables)
        for el in to_delete:
            db, tbl = el.split(".")
            print(f"Deleting Glue Table '{db}.{tbl}'...")
            api.delete_glue_table(db, tbl)

    elif selected == GlueOptions.REPAIR:
        for db, tables in created_tables.items():
            for table in tables:
                name = table["Name"]
                print(f"Checking '{db}.{name}'... ", end="", flush=True)
                s3db_type = {
                    el["Name"]: el["Type"]
                    for el in api.get_glue_type_map_from_s3db(db, name)
                }
                table_type = {
                    el["Name"]: el["Type"]
                    for el in table["StorageDescriptor"]["Columns"]
                }
                if s3db_type != table_type:
                    print("mismatch found:")
                    print(f"   s3db: {s3db_type}")
                    print(f"  table: {table_type}")
                    if prompt_confirmation("Repair Glue Table?"):
                        print(f"Updating '{db}.{name}'... ", end="", flush=True)
                        api.create_glue_table(db, name, update=True)
                        print("done")
                else:
                    print("done")

    else:
        raise Exception(f"Unhandled selection: {selected}")


def prompt_text(text, default=None, validate=None) -> str:
    args = {
        "name": "data",
        "type": "input",
        "message": text,
    }
    if default:
        args["default"] = default
    if validate:
        args["validate"] = validate
    return prompt([args])["data"]


def prompt_options(text: str, choices: list[str]) -> str:
    args = {
        "name": "data",
        "type": "list",
        "message": text,
        "choices": choices,
    }
    return prompt([args])["data"]


def prompt_checkbox(text: str, choices: list[str]) -> list[str]:
    args = {
        "name": "data",
        "type": "checkbox",
        "message": text,
        "choices": [{"name": c} for c in choices],
    }
    return prompt([args])["data"]


def prompt_confirmation(text: str, default=True) -> list[str]:
    args = {
        "name": "data",
        "type": "confirm",
        "default": default,
        "message": text,
    }
    return prompt([args])["data"]


if __name__ == "__main__":
    main()

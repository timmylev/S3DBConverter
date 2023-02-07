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
    get_s3db_type_map,
    list_collections,
    list_datasets,
)
from lambdas.prod_listener import LIVE_STORES


ACCOUNTS = {
    "production": {
        "id": 516256908252,
        "role": "arn:aws:iam::516256908252:role/Admin",
    },
    "services": {
        "id": 534964971383,
        "role": "arn:aws:iam::534964971383:role/Admin",
    },
}

# Glue Catalog/Table and Athena Configs
ATHENA_DEFAULT_ACCOUNT = "services"
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
    CREATE = "Check for New S3DB Datasets"
    REPAIR = "Check/Repair Table Schemas"
    DELETE = "Delete Tables"
    BACK = "BACK"
    EXIT = "EXIT"


class BackfillRange(str, Enum):
    ALL = "All files"
    LATEST_N = "Latest N files"


class Options(str, Enum):
    BACKFILLS = "Trigger S3DB Conversions"
    MANAGE_ATHENA = "Manage AthenaDB"
    EXIT = "EXIT"


def main():
    print_welcome()

    aws_id = boto3.client("sts").get_caller_identity()["Account"]
    print(f"Running on account '{aws_id}'...")

    api = API(prompt_text("Stack Name:", default=STACK_NAME))

    while True:
        action = prompt_options(
            "What would you like to do:", [i.value for i in Options]
        )

        if action == Options.BACKFILLS:
            prompt_backfills(api)

        elif action == Options.MANAGE_ATHENA:
            default = f"{ATHENA_DEFAULT_ACCOUNT.upper()} Accont (default)"
            profiles = boto3.session.Session().available_profiles
            options = [default, Separator("===== available profiles ====="), *profiles]
            profile = prompt_options("Select AWS account/profile for Athena", options)
            profile = None if profile == default else profile

            if api.athena_acc_profile != profile:
                api = API(api.stack_name, athena_account_profile=profile)

            prompt_athena_manager(api)

        elif action == Options.EXIT:
            print("Terminating...")
            sys.exit(0)


class API:
    def __init__(self, stack_name, athena_account_profile=None):
        self.stack_name = stack_name
        self._stack_outputs = None

        self.aws_sesh = boto3.session.Session()

        resp = self.aws_sesh.client("sts").get_caller_identity()
        self.curr_account = int(resp["Account"])

        self._prod_sesh = None
        self.athena_acc_profile = athena_account_profile
        self._athena_acc_sesh = None

        self._cfn = None
        self._lmb = None
        self._glue = None

    @property
    def prod_sesh(self):
        if self._prod_sesh is None:
            if self.curr_account != ACCOUNTS["production"]["id"]:
                prod_admin = ACCOUNTS["production"]["role"]
                print(f"Getting AWS sesh using production role '{prod_admin}'...")
                self._prod_sesh, _ = self._assume_iam_role(prod_admin, "s3dbcli")

            else:
                self._prod_sesh = boto3.session.Session()

        return self._prod_sesh

    @property
    def athena_acc_sesh(self):
        if self._athena_acc_sesh is None:
            if self.athena_acc_profile:
                print(f"Getting AWS sesh using profile '{self.athena_acc_profile}'...")
                self._athena_acc_sesh = boto3.session.Session(
                    profile_name=self.athena_acc_profile
                )

            elif self.curr_account != ACCOUNTS[ATHENA_DEFAULT_ACCOUNT]["id"]:
                role = ACCOUNTS[ATHENA_DEFAULT_ACCOUNT]["role"]
                print(
                    f"Getting AWS sesh using {ATHENA_DEFAULT_ACCOUNT} role '{role}'..."
                )
                self._athena_acc_sesh, _ = self._assume_iam_role(role, "s3dbcli")
            else:
                self._athena_acc_sesh = boto3.session.Session()

        return self._athena_acc_sesh

    @property
    def cfn(self):
        if self._cfn is None:
            self._cfn = self.prod_sesh.client("cloudformation")
        return self._cfn

    @property
    def lmb(self):
        if self._lmb is None:
            self._lmb = self.prod_sesh.client("lambda")
        return self._lmb

    @property
    def glue(self):
        if self._glue is None:
            self._glue = self.athena_acc_sesh.client("glue")
        return self._glue

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

        available = {db: list_datasets(db) for db in s3db_colls}
        created = {db: list(self.list_glue_tables(db)) for db in glue_dbs}
        missing = {
            db: set(available[db]) - set([t["Name"] for t in created.get(db, [])])
            for db in s3db_colls
        }
        return created, missing

    def list_glue_databases(self):
        s3db_colls = set(list_collections())
        glue_dbs = set([el["Name"] for el in self.glue.get_databases()["DatabaseList"]])
        return sorted(s3db_colls & glue_dbs)

    def list_glue_tables(self, db):
        paginator = self.glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=db):
            yield from page["TableList"]

    def create_glue_table(self, db, table, update=False):
        print(f"Creating Glue Table '{db}.{table}'...")
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

    def create_glue_database(self, db):
        self.glue.create_database(DatabaseInput={"Name": db})

    def delete_glue_table(self, db, table):
        self.glue.delete_table(DatabaseName=db, Name=table)

    def get_glue_type_map_from_s3db(self, db, table):
        s3db_types = get_s3db_type_map(db, table)
        # We have no way of knowing if the dataset uses big/small int/float because
        # the S3DB metadata doesn't specify it. Opting for the smaller size during
        # Glue schema creation leads to an error when querying later via Athena.
        # eg: 'GENERIC_INTERNAL_ERROR: Value 2156111684 exceeds MAX_INT'
        overrides = {
            "target_start": "bigint",
            "target_end": "bigint",
            "release_date": "bigint",
            "target_bounds": "tinyint",
        }
        conversions = {
            "str": "string",
            "int": "bigint",
            "float": "double",
            "bool": "boolean",
            "timedelta": "double",
            "datetime": "bigint",
            "list": "string",
            "tuple": "string",
        }
        return [
            {"Name": k, "Type": overrides.get(k, conversions.get(v, v))}
            for k, v in s3db_types.items()
        ]

    def _assume_iam_role(
        self,
        role_arn: str,
        sesh_name: str,
        sesh_duration: int = 3600,
    ):
        response = boto3.client("sts").assume_role(
            RoleArn=role_arn,
            RoleSessionName=sesh_name,
            DurationSeconds=sesh_duration,
        )
        credentials = response["Credentials"]

        sesh = boto3.session.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
        sesh_expiry = credentials["Expiration"]

        return sesh, sesh_expiry


def print_welcome():
    cprint(Figlet(font="big").renderText("S3DB   CLI"), "blue")
    print("\nWelcome to S3DB CLI!\n")
    print(" S3DB Source:")
    cprint(f"   s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}", "green")
    print(f" Live Conversions ({len(LIVE_STORES)}):")
    for el in LIVE_STORES:
        uri = f"s3://{SOURCE_BUCKET}/{el['dest_prefix']}"
        cprint(f"   {uri}  ", "green", end="")
        cprint(f"({el['dest_store']})", "blue")
    print(" Athena Source:")
    cprint(f"   {DATA_LOCATION}", "green")
    print("")


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
            n_files = int(prompt_text("Specify number of files:"))
        else:
            n_files = None

        num_datasets = sum([len(v) for v in targets.values()])
        msg = f"Backfilling {num_datasets} datasets. Proceed?"
        if prompt_confirmation(msg):
            print("Trigerring Request Generator... ")

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

            print("Done")
        else:
            print("Cancelling...")


def prompt_athena_manager(api):
    while True:
        o = prompt_options("What would you like to do:", [i.value for i in GlueOptions])

        if o == GlueOptions.CREATE:
            print("Checking for new S3DB datasets...")
            created, missing = api.check_glue_catalog()

            if any([tbs for tbs in missing.values()]):
                print("New datasets detected:")
                for db in sorted(missing.keys()):
                    tables = missing[db]
                    if tables:
                        print(f"  {db:9}: {sorted(tables)}")

                tables = sorted([f"{db}.{t}" for db, ts in missing.items() for t in ts])
                to_create = prompt_checkbox("Select tables to create:", tables)

                for el in to_create:
                    db, tbl = el.split(".")

                    if db not in created:
                        print(f"Creating database '{db}'...")
                        api.create_glue_database(db)

                    api.create_glue_table(db, tbl)

            else:
                print("No new datasets detected.")

        elif o == GlueOptions.DELETE:
            print("Loading tables... ")
            tables = [
                f"{db}.{t['Name']}"
                for db in api.list_glue_databases()
                for t in api.list_glue_tables(db)
            ]
            if tables:
                to_delete = prompt_checkbox("Select tables:", sorted(tables))
                for el in to_delete:
                    db, tbl = el.split(".")
                    print(f"Deleting Glue Table '{db}.{tbl}'...")
                    api.delete_glue_table(db, tbl)
            else:
                print("No tables found!")

        elif o == GlueOptions.REPAIR:
            for db in api.list_glue_databases():
                for table in api.list_glue_tables(db):
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
                        print("FAILED - Schema mismatch found!")
                        print(f"   s3db: {s3db_type}")
                        print(f"  table: {table_type}")
                        if prompt_confirmation("Repair Glue Table?"):
                            print(f"Updating '{db}.{name}'... ", end="", flush=True)
                            api.create_glue_table(db, name, update=True)
                            print("done")
                    else:
                        print("SUCCESS")

        elif o == GlueOptions.BACK:
            break

        elif o == GlueOptions.EXIT:
            print("Terminating...")
            sys.exit(0)

        else:
            raise Exception(f"Unhandled selection: {o}")


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

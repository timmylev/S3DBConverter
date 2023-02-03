import json
import os
import sys
from enum import Enum

import boto3
from PyInquirer import Separator, prompt

from deploy import STACK_NAME
from lambdas.common import (
    COMPRESSION,
    COMPRESSION_LEVELS,
    COMPRESSION_LEVELS_DEFAULTS,
    DEST_STORES,
    FILE_FORMATS,
    PARTITION_SIZES,
    list_collections,
    list_datasets,
)


PROD_ACCOUNT = 516256908252


class BackfillRange(str, Enum):
    ALL = "All files"
    LATEST_N = "Latest N files"


class Options(str, Enum):
    BACKFILLS = "Trigger S3DB Conversions"
    EXIT = "Exit"


def main():
    print("-------------- S3DB Converter CLI --------------")

    api = API(prompt_text("Stack Name:", default=STACK_NAME))

    while True:
        action = prompt_options(
            "What would you like to do:", [i.value for i in Options]
        )

        if action == Options.BACKFILLS:
            prompt_backfills(api)

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

    partition = prompt_options("Select dest partition size:", PARTITION_SIZES)

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

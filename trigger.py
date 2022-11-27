import json
import os
import sys
from enum import Enum
from typing import Optional

import boto3
from PyInquirer import Separator, prompt

from deploy import STACK_NAME
from lambdas.common import COMPRESSION, list_collections, list_datasets


PROD_ACCOUNT = 516256908252
PARTITIONS = ["day", "month", "year"]


class Options(str, Enum):
    BACKFILLS = "Trigger S3DB Conversions"
    EXIT = "Exit"


def main():
    print("-------------- S3DB Converter CLI --------------")

    api = API(prompt_text("Stack Name:", default=STACK_NAME))

    while True:
        action = prompt_options("What would you like to do", [i.value for i in Options])

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

        return self._retriever_outputs

    def trigger_lambda(self, dest_prefix, compression, partition, datasets):
        event = {
            "dest_prefix": dest_prefix,
            "compression": compression,
            "partition": partition,
            "datasets": datasets,
        }

        self.lmb.invoke(
            FunctionName=self.stack_outputs["RequestGeneratorFunctionName"],
            InvocationType="Event",
            Payload=json.dumps(event),
        )


def prompt_backfills(api):
    compression = prompt_options("Select dest compression:", COMPRESSION)
    partition = prompt_options("Select dest partition:", PARTITIONS)

    if partition == "year":
        print(
            "WARNING: Partitioning by year will fail for very large datasets such as "
            "CAISO Price Data due to AWS Lambda hitting max memory (10GB)."
        )

    dest_prefix = prompt_text(
        "Specify dest s3 prefix",
        default="/".join(["version5", "arrow", compression, partition, ""]),
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

    if targets:
        print("Trigerring Request Generator...", end="", flush=True)

        for coll, ds in targets.items():
            if ds:
                api.trigger_lambda(dest_prefix, compression, partition, {coll: ds})

        print(" Done")


def prompt_text(text: str, default: Optional[str] = None) -> str:
    args = {
        "name": "data",
        "type": "input",
        "message": text,
    }
    if default:
        args["default"] = default
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


if __name__ == "__main__":
    main()

import json
import os
from typing import Optional

import boto3
from PyInquirer import prompt

from deploy import STACK_NAME
from lambdas.common import COMPRESSION, list_collections, list_datasets


def main():
    print("-------------- S3DB Converted CLI --------------")
    print("Ensure that you've assumed the prod account role.")

    stack_name = prompt_text("Stack Name:", default=STACK_NAME)
    compression = prompt_options("Select dest compression:", COMPRESSION)
    partition = prompt_options("Select dest partition:", ["day", "month", "year"])
    dest_prefix = prompt_text(
        "Specify dest s3 prefix",
        default=default_dest_prefix("arrow", compression, "day"),
    )
    dest_prefix = os.path.join(dest_prefix, "")

    targets = {}
    collections = list_collections()

    while True:
        coll = prompt_options("Select collection:", ["DONE", "ALL", *collections])
        if coll == "DONE":
            break
        elif coll == "ALL":
            targets = {c: list_datasets(c) for c in collections}
            break
        else:
            ds = prompt_checkbox("Select dataset(s):", list_datasets(coll))
            targets[coll] = sorted(set([*ds, *targets.get(coll, [])]))

    print("Trigerring Request Generator...")

    for coll, ds in targets.items():
        if ds:
            trigger(stack_name, dest_prefix, compression, partition, {coll: ds})

    print("Done")


def trigger(stack_name, dest_prefix, compression, partition, datasets):
    event = {
        "dest_prefix": dest_prefix,
        "compression": compression,
        "partition": partition,
        "datasets": datasets,
    }

    outputs = get_stack_outputs(stack_name)

    boto3.client("lambda").invoke(
        FunctionName=outputs["RequestGeneratorFunctionName"],
        InvocationType="Event",
        Payload=json.dumps(event),
    )


def get_stack_outputs(stack_name):
    resp = boto3.client("cloudformation").describe_stacks(StackName=stack_name)
    return {el["OutputKey"]: el["OutputValue"] for el in resp["Stacks"][0]["Outputs"]}


def default_dest_prefix(fmt, com, part):
    return "/".join(["version5", fmt, com, part, ""])


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

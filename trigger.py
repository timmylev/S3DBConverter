import json
import os
from typing import Optional

import boto3
from PyInquirer import prompt

from deploy import STACK_NAME
from lambdas.common import list_collections, list_datasets


def main():
    print("-------------- S3DB Converted CLI --------------")
    print("Ensure that you're assumed the prod account role.")

    stack_name = prompt_text("Stack Name:", default=STACK_NAME)
    dest_prefix = os.path.join(prompt_text("Destination S3 Prefix"), "")

    targets = {}
    collections = list_collections()

    while True:
        coll = prompt_options("Select collection:", ["DONE", "ALL", *collections])
        if coll == "DONE":
            break
        elif coll == "ALL":
            targets = "all"
            break
        else:
            ds = prompt_checkbox("Select dataset(s):", list_datasets(coll))
            targets[coll] = sorted(set([*ds, *targets.get(coll, [])]))

    print("Trigerring Request Generator...")
    trigger(stack_name, dest_prefix, targets)
    print("Done")


def trigger(stack_name, dest_prefix, datasets):
    event = {
        "dest_prefix": dest_prefix,
        "compression": "zst",  # only zst is suported currently
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

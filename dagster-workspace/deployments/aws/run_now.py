#!/usr/bin/env python
"""Trigger an on-demand run of the Montreal pipeline task.

`cdk deploy` (app.py) provisions the *monthly* schedule; this fires the same
Fargate task **now**, without waiting for the 1st. It reads the stack's
CloudFormation outputs, so there are no ARNs to copy by hand.

    python run_now.py            # uses stack DagsterMonthlyJobStack
    STACK_NAME=other python run_now.py

Region/credentials come from the usual AWS env (AWS_REGION / AWS_PROFILE).
"""

from __future__ import annotations

import os
import sys

import boto3

STACK_NAME = os.environ.get("STACK_NAME", "DagsterMonthlyJobStack")


def stack_outputs(stack_name: str) -> dict[str, str]:
    """The stack's CloudFormation outputs as a flat ``{key: value}`` dict."""
    stacks = boto3.client("cloudformation").describe_stacks(StackName=stack_name)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def main() -> int:
    out = stack_outputs(STACK_NAME)
    response = boto3.client("ecs").run_task(
        cluster=out["ClusterName"],
        taskDefinition=out["TaskDefinitionArn"],
        launchType="FARGATE",
        count=1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": out["PublicSubnetIds"].split(","),
                "securityGroups": [out["TaskSecurityGroupId"]],
                "assignPublicIp": "ENABLED",
            }
        },
    )

    failures = response.get("failures") or []
    if failures:
        print(f"run-task failed: {failures}", file=sys.stderr)
        return 1

    task_arn = response["tasks"][0]["taskArn"]
    print(f"Started task {task_arn}")
    print("Logs: CloudWatch log group 'dagster-monthly/...'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

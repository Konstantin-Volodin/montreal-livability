#!/usr/bin/env python
"""Fire the monthly run now, instead of waiting for the 1st.

Starts the Step Functions state machine (which launches the Fargate Spot task and
retries on interruption), reading the stack's CloudFormation outputs:

    python run.py                # stack LivabilityStack
    STACK_NAME=other python run.py

"""

from __future__ import annotations

import os
import boto3

STACK_NAME = os.environ.get("STACK_NAME", "LivabilityStack")
REGION = os.environ.get("AWS_REGION", "ca-central-1")

def stack_outputs(stack_name: str) -> dict[str, str]:
    """The stack's CloudFormation outputs as a flat ``{key: value}`` dict."""
    stacks = boto3.client("cloudformation", region_name=REGION).describe_stacks(StackName=stack_name)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


def main() -> int:
    out = stack_outputs(STACK_NAME)
    response = boto3.client("stepfunctions", region_name=REGION).start_execution(
        stateMachineArn=out["StateMachineArn"]
    )
    print(f"Started execution {response['executionArn']}")
    print("Logs: CloudWatch log group /ecs/livability")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

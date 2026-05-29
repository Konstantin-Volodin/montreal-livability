# AWS deployment — monthly Fargate batch run

Runs the Montreal livability pipeline once a month on AWS, with no always-on
infrastructure.

```
EventBridge Scheduler  ──(cron, monthly)──▶  ECS RunTask (Fargate)
                                                  │
                                                  ▼
                                   one-shot `montreal` container
                                   runs the staged pipeline, writes
                                   the S3 lakehouse, then exits
```

You pay only for the few minutes the task runs each month (plus pennies of ECR
storage). No NAT gateway, no webserver, no daemon.

## How the run works

The pipeline is a pure asset graph with a runtime twist, so the container does
**not** just run `dagster asset materialize '*'`. The entrypoint
([`montreal.batch`](../../projects/montreal/src/montreal/batch.py), run as
`python -m montreal.batch`) materializes it in stages:

1. `montreal_addresses` — writes/refreshes the address snapshot.
2. `h3_montreal_addresses` — H3-indexes addresses and **registers the dynamic
   `address_r6` partitions**.
3. `+amenities`, `+montreal_municipalities` — the rest of the unpartitioned
   upstream.
4. `distances_to_amenities` — once per registered r6 partition, run
   `PARTITION_CONCURRENCY`-at-a-time (defaults to the task's vCPU count).
5. `livability_score`, then `livability_map` — gold aggregation + HTML report.

All stages share one SQLite Dagster instance under `$DAGSTER_HOME` inside the
container, so the partitions registered in step 2 are visible in step 4. That
instance is throwaway; the durable output is the S3 lakehouse.

**Parallelism.** Steps within an unpartitioned stage (e.g. the H3 indexes +
amenities) run in parallel via the `multiprocess_executor` set on the
`Definitions`. The partitioned distance stage is parallelized by launching
concurrent *runs* (Dagster does one partition per run), bounded by
`PARTITION_CONCURRENCY` to stay clear of SQLite write contention on the shared
throwaway instance.

## Configuration

Resolved as **CDK context (`-c key=value`) → env var → default**:

| Setting | Context key / env var | Default |
|---|---|---|
| Data bucket (lakehouse) | `data_bucket` / `S3_BUCKET` | `aws-dagster-example` |
| Bucket region | `data_region` / `S3_REGION` | `us-east-1` |
| Schedule (6-field cron, UTC) | `schedule_expression` / `SCHEDULE_EXPRESSION` | `cron(0 6 1 * ? *)` |

The bucket is assumed to already exist (it is not created or deleted by this
stack). The task role is granted read/write on it; auth uses the task role via
boto3's default credential chain (no static keys in the image). Deploy the
stack in the **same region as the bucket** so the free S3 gateway endpoint
applies.

## Prerequisites

- **Docker** running locally (CDK builds the image at deploy time).
- **Node.js + AWS CDK CLI**: `npm install -g aws-cdk`.
- AWS credentials configured (`aws configure` / `AWS_PROFILE`), and the account
  bootstrapped for CDK once: `cdk bootstrap`.
- A Python env with the CDK library:

```bash
cd dagster-workspace/deployments/aws
python -m venv .venv && . .venv/bin/activate   # PowerShell: .venv\Scripts\Activate.ps1
pip install -e .          # aws-cdk-lib + constructs + boto3 (for run_now.py)
```

## Deploy

```bash
cd dagster-workspace/deployments/aws
export AWS_REGION=us-east-1        # match the bucket's region

cdk synth                          # render CloudFormation (no AWS changes)
cdk deploy                         # build image, push to ECR, create the stack

# Override settings at deploy time, e.g. a different bucket + weekly schedule:
cdk deploy -c data_bucket=my-bucket -c "schedule_expression=cron(0 6 ? * MON *)"
```

## Trigger a run manually (don't wait for the 1st)

The two operator entrypoints are **`cdk deploy`** (provision/refresh the monthly
job) and **`run_now.py`** (fire a run on demand). The latter reads the stack
outputs itself, so there are no ARNs to copy:

```bash
cd dagster-workspace/deployments/aws
python run_now.py                 # or: STACK_NAME=<name> python run_now.py
```

Equivalent raw CLI, if you'd rather not use the script (values are the
`cdk deploy` outputs):

```bash
aws ecs run-task \
  --cluster <ClusterName> \
  --task-definition <TaskDefinitionArn> \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<PublicSubnetIds>],securityGroups=[<TaskSecurityGroupId>],assignPublicIp=ENABLED}"
```

Either way, follow the run in the `dagster-monthly/...` CloudWatch log group
(1-month retention) — that is the only log sink; the in-container SQLite
instance and its logs die with the task.

## Tear down

```bash
cdk destroy
```

This removes the schedule, cluster, VPC, roles, and log group. The data bucket
and its contents are left untouched. Old image versions remain in the
CDK-managed ECR repo; `cdk gc` (or the console) can prune them.

## Notes / trade-offs

- **Public subnet + public IP** (instead of private subnet + NAT) keeps standing
  cost at zero. Nothing inbound is permitted; egress is open for dataset
  downloads.
- **Per-partition asset checks are re-read in full.** Each `distances_to_amenities`
  partition run re-evaluates its checks, which read every shard written so far.
  Harmless (checks are non-blocking) but O(partitions²) reads — fine monthly. A
  future move to the Dagster daemon + a scheduled backfill would avoid it and
  add the Dagster UI / run history, at the cost of always-on compute.
- Sizing is `2 vCPU / 8 GB`; bump `cpu` / `memory_limit_mib` in `app.py` if the
  gold aggregation (which loads all addresses at once) runs short on memory.

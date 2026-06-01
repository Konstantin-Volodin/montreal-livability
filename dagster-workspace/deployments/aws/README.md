# AWS deployment - monthly Fargate batch

Runs the Montreal livability pipeline once a month, all in `ca-central-1`.

*no always-on infra*. Pay only for the minutes the task runs.

```
EventBridge Scheduler ──(monthly, 1 AM Eastern)──▶ ECS RunTask (Fargate, on-demand)
                                                     └─ one-shot `montreal` container:
                                                        runs the pipeline, writes S3, exits
```

## Layout

| File                                      | What                                                            |
| ----------------------------------------- | --------------------------------------------------------------- |
| `app.py`                                  | the CDK stack - VPC, cluster, task def, EFS, schedule           |
| `run.py`                                  | fire a run on demand (`ecs:RunTask`)                            |
| `cdk.json` · `pyproject.toml` · `uv.lock` | CDK + Python config                                             |

## Config

Resolved as CDK context (`-c key=value`) → env var → default:

| Setting             | Key / env                                     | Default               |
| ------------------- | --------------------------------------------- | --------------------- |
| Data bucket         | `data_bucket` / `S3_BUCKET`                   | `montreal-livability` |
| Region              | `data_region` / `S3_REGION`                   | `ca-central-1`        |
| Schedule (cron)     | `schedule_expression` / `SCHEDULE_EXPRESSION` | `cron(0 1 1 * ? *)`   |
| Schedule state      | `schedule_state` / `SCHEDULE_STATE`           | `ENABLED`             |
| Alert email         | `alert_email` / `ALERT_EMAIL`                 | (see `app.py`)        |

`cron(0 1 1 * ? *)` is the 1st of the month at **01:00 `America/Toronto`** (Eastern,
DST-aware). The bucket must already exist (this stack never creates or deletes it).
Everything deploys in `ca-central-1` so the free S3 gateway endpoint applies.

## One-time setup

Needs Docker running (CDK builds the image at deploy).

```powershell
uv sync                                                   # Python env
npm install -g aws-cdk                                    # CDK CLI
cdk bootstrap aws://<account-id>/<region>                 # cloud formation for AWS account
aws s3 mb s3://montreal-livability --region <region>      # s3 bucket
```

## Deploy

```powershell
cdk deploy                                    # builds image -> ECR repo `montreal:<version>`, creates the stack
```

Prints `ClusterArn` / `TaskDefinitionArn` / `Subnets` / `SecurityGroupId` (consumed by `run.py`).

## run, pause, tear down

```powershell
python run.py                                 # fire a run now, don't wait for the 1st
aws logs tail /ecs/livability --follow        # follow it in CloudWatch

cdk deploy -c schedule_state=DISABLED         # pause the monthly run (run.py still works)
cdk deploy                                    # resume

cdk destroy                                   # remove everything except the data bucket
```

## Data quality

Check verdicts are read from the Dagster event log (on the EFS-persisted instance) at the
end of each run — latest-per-partition, so a failed shard isn't masked by a passing one —
and assembled into `s3://<bucket>/quality/<run>.json` plus a summary in the CloudWatch logs.
Any ERROR-severity failure is emailed via SNS - **confirm the subscription once** by clicking
the link in the first "AWS Notification - Subscription Confirmation" mail (check spam).
Failures are recorded and emailed, not gated: the batch still finishes.

## Notes

- Public subnet + public IP, no NAT - zero standing cost, egress only.
- Runs on **on-demand Fargate** (no Spot), launched straight from EventBridge Scheduler — no state machine. A missed/failed month just reruns on the next schedule or via `run.py`; reruns are idempotent thanks to the S3 cache + EFS-persisted instance.
- 2 vCPU / 8 GB; bump `cpu` / `memory_limit_mib` in `app.py` if the gold step runs short on memory.
- Image is published to a named ECR repo as `montreal:<version>` (from the project's `pyproject.toml`) - trackable, not a content hash.

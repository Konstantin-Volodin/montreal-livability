# AWS deployment — monthly Fargate batch

Runs the Montreal livability pipeline once a month. 

*no always-on infra*. Pay only for the minutes the task runs.

```
EventBridge Scheduler ──(monthly)──▶ ECS RunTask (Fargate)
                                         └─ one-shot `montreal` container
                                            runs the pipeline, writes S3, exits
```

## Layout

| File                                      | What                                             |
| ----------------------------------------- | ------------------------------------------------ |
| `app.py`                                  | the CDK stack — VPC, cluster, task def, schedule |
| `run.py`                                  | fire a run on demand                             |
| `cdk.json` · `pyproject.toml` · `uv.lock` | CDK + Python config                              |

## Config

Resolved as CDK context (`-c key=value`) → env var → default:

| Setting             | Key / env                                     | Default               |
| ------------------- | --------------------------------------------- | --------------------- |
| Data bucket         | `data_bucket` / `S3_BUCKET`                   | `montreal-livability` |
| Region              | `data_region` / `S3_REGION`                   | `ca-central-1`        |
| Schedule (UTC cron) | `schedule_expression` / `SCHEDULE_EXPRESSION` | `cron(0 1 1 * ? *)`   |
| Schedule state      | `schedule_state` / `SCHEDULE_STATE`           | `ENABLED`             |

The bucket must already exist (this stack never creates or deletes it). Deploy in
the bucket's region so the free S3 gateway endpoint applies.

## One-time setup

Needs Docker running (CDK builds the image at deploy).

```powershell
uv sync                                       # Python env
npm install -g aws-cdk                        # CDK CLI
cdk bootstrap aws://<account-id>/<region>
aws s3 mb s3://montreal-livability --region <region>
```

## Deploy

```powershell
cdk deploy                                    # builds image -> ECR repo `montreal:<version>`, creates the stack
```

Prints `ClusterName`, `TaskDefinitionArn`, `TaskSecurityGroupId`, `PublicSubnetIds`.

## run, pause, tear down

```powershell
python run.py                                 # fire a run now, don't wait for the 1st
aws logs tail livability --follow             # follow it in CloudWatch

cdk deploy -c schedule_state=DISABLED         # pause the monthly run (run.py still works)
cdk deploy                                    # resume

cdk destroy                                   # remove everything except the data bucket
```

## Notes

- Public subnet + public IP, no NAT — zero standing cost, egress only.
- 2 vCPU / 8 GB; bump `cpu` / `memory_limit_mib` in `app.py` if the gold step runs short on memory.
- Image is published to a named ECR repo as `montreal:<version>` (from the project's `pyproject.toml`) — trackable, not a content hash.

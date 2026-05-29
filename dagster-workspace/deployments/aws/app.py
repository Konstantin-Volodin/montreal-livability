"""CDK app: run the Montreal livability pipeline as a monthly Fargate batch job.

Shape:
  EventBridge Scheduler (cron, monthly)
    -> ECS RunTask (Fargate)
    -> one-shot container (montreal image) runs the staged pipeline
    -> reads/writes the S3 lakehouse via the task IAM role
    -> exits.

No always-on infrastructure: you pay only for the minutes the task runs each
month. The container image is built from the montreal project (see PROJECT_DIR)
by CDK and pushed to ECR on deploy.
"""

import os
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
    aws_scheduler as scheduler,
)
from constructs import Construct

# Docker build context: dagster-workspace/projects/montreal. This file lives at
# dagster-workspace/deployments/aws/app.py, so parents[2] is the workspace root.
PROJECT_DIR = Path(__file__).resolve().parents[2] / "projects" / "montreal"
assert PROJECT_DIR.is_dir(), f"montreal project not found at {PROJECT_DIR}"


class DagsterMonthlyJobStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_bucket: str,
        data_region: str,
        schedule_expression: str,
        cpu: int = 2048,
        memory_limit_mib: int = 8192,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc, cluster = self._network()
        task_definition = self._task_definition(
            data_bucket, data_region, cpu=cpu, memory_limit_mib=memory_limit_mib
        )
        task_sg = self._schedule(vpc, cluster, task_definition, schedule_expression)

        # Outputs are consumed by run_now.py (on-demand trigger) and a manual
        # `aws ecs run-task`; everything that command needs is exported here.
        cdk.CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        cdk.CfnOutput(
            self, "TaskDefinitionArn", value=task_definition.task_definition_arn
        )
        cdk.CfnOutput(self, "TaskSecurityGroupId", value=task_sg.security_group_id)
        cdk.CfnOutput(
            self,
            "PublicSubnetIds",
            value=",".join(s.subnet_id for s in vpc.public_subnets),
        )

    def _network(self) -> tuple[ec2.Vpc, ecs.Cluster]:
        """Public-subnet-only VPC (no NAT) + S3 gateway endpoint, and the cluster.

        nat_gateways=0 is the cost play; the S3 gateway endpoint lets the task
        reach the lakehouse for free without an internet path.
        """
        vpc = ec2.Vpc(
            self, "DagsterVpc", max_azs=2, nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24)
            ],
        )
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
        cluster = ecs.Cluster(self, "DagsterCluster", vpc=vpc)
        return vpc, cluster

    def _task_definition(
        self, data_bucket: str, data_region: str, *, cpu: int, memory_limit_mib: int
    ) -> ecs.FargateTaskDefinition:
        """Fargate task def: the montreal image, lakehouse IAM, and CloudWatch logs."""
        # Image built from the montreal project and pushed to ECR on deploy.
        image = ecs.ContainerImage.from_asset(
            str(PROJECT_DIR),
            file="Dockerfile",
            platform=ecr_assets.Platform.LINUX_AMD64,
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "DagsterTaskDef",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
        )

        # The pipeline reaches the lakehouse through the task role (boto3's
        # default credential chain). S3 splits object ARNs (/*) from bucket ARNs.
        bucket_arn = f"arn:aws:s3:::{data_bucket}"
        task_definition.task_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                resources=[f"{bucket_arn}/*"],
            )
        )
        task_definition.task_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:GetBucketLocation"],
                resources=[bucket_arn],
            )
        )

        task_definition.add_container(
            "DagsterJob",
            image=image,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="dagster-monthly",
                log_retention=logs.RetentionDays.ONE_MONTH,
            ),
            environment={
                "S3_BUCKET": data_bucket,
                "S3_REGION": data_region,
                # Blank keys -> the s3_datastore resource falls back to the task
                # role via boto3's default credential chain (no static secrets).
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
            },
        )
        return task_definition

    def _schedule(
        self,
        vpc: ec2.Vpc,
        cluster: ecs.Cluster,
        task_definition: ecs.FargateTaskDefinition,
        schedule_expression: str,
    ) -> ec2.SecurityGroup:
        """Monthly EventBridge Scheduler -> ECS RunTask. Returns the task's SG."""
        # EventBridge Scheduler assumes this role to launch the task and to pass
        # the task/execution roles to ECS (the PassRole is easy to forget).
        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=[task_definition.task_definition_arn],
                conditions={"ArnLike": {"ecs:cluster": cluster.cluster_arn}},
            )
        )
        scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[
                    task_definition.task_role.role_arn,
                    task_definition.execution_role.role_arn,
                ],
                conditions={
                    "StringLike": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}
                },
            )
        )

        task_sg = ec2.SecurityGroup(
            self, "TaskSg", vpc=vpc, allow_all_outbound=True, description="Dagster monthly task"
        )

        scheduler.CfnSchedule(
            self,
            "MonthlySchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF"
            ),
            schedule_expression=schedule_expression,
            schedule_expression_timezone="UTC",
            target=scheduler.CfnSchedule.TargetProperty(
                arn=cluster.cluster_arn,
                role_arn=scheduler_role.role_arn,
                ecs_parameters=scheduler.CfnSchedule.EcsParametersProperty(
                    task_definition_arn=task_definition.task_definition_arn,
                    launch_type="FARGATE",
                    task_count=1,
                    network_configuration=scheduler.CfnSchedule.NetworkConfigurationProperty(
                        awsvpc_configuration=scheduler.CfnSchedule.AwsVpcConfigurationProperty(
                            subnets=[s.subnet_id for s in vpc.public_subnets],
                            security_groups=[task_sg.security_group_id],
                            assign_public_ip="ENABLED",
                        )
                    ),
                ),
                # A monthly batch should not be retried into a duplicate run.
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(
                    maximum_retry_attempts=0
                ),
            ),
        )
        return task_sg


app = cdk.App()


def _setting(key: str, env_var: str, default: str) -> str:
    """CDK context (-c key=value) wins, then an env var, then the default."""
    return app.node.try_get_context(key) or os.environ.get(env_var) or default


DagsterMonthlyJobStack(
    app,
    "DagsterMonthlyJobStack",
    data_bucket=_setting("data_bucket", "S3_BUCKET", "aws-dagster-example"),
    data_region=_setting("data_region", "S3_REGION", "us-east-1"),
    # 06:00 UTC on the 1st of every month (EventBridge Scheduler cron: 6 fields).
    schedule_expression=_setting(
        "schedule_expression", "SCHEDULE_EXPRESSION", "cron(0 6 1 * ? *)"
    ),
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
)
app.synth()

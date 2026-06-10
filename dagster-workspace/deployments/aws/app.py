"""
Monthly Fargate batch. No always-on infra.
EventBridge Scheduler -> ECS RunTask -> one-shot `montreal` container -> exits.
"""

import os
import re
from pathlib import Path

import aws_cdk as cdk
import cdk_ecr_deployment as ecr_deployment
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecr_assets as ecr_assets,
    aws_ecs as ecs,
    aws_efs as efs,
    aws_iam as iam,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
)
from constructs import Construct

# Docker build context (workspace root is parents[2] from this file).
PROJECT_DIR = Path(__file__).resolve().parents[2] / "projects" / "montreal"
assert PROJECT_DIR.is_dir(), f"montreal project not found at {PROJECT_DIR}"


def _project_meta() -> tuple[str, str]:
    """(name, version) from the montreal pyproject - used as the ECR repo + tag."""
    text = (PROJECT_DIR / "pyproject.toml").read_text()
    return (
        re.search(r'(?m)^name\s*=\s*"([^"]+)"', text)[1],
        re.search(r'(?m)^version\s*=\s*"([^"]+)"', text)[1],
    )
IMAGE_NAME, IMAGE_VERSION = _project_meta()


class LivabilityStack(cdk.Stack):
    """manifest: network -> state -> image -> alerts -> task -> schedule."""

    def __init__(
        self, scope: Construct, construct_id: str,
        *,
        data_bucket: str, data_region: str,
        schedule_expression: str, schedule_state: str = "ENABLED",
        alert_email: str = "",
        cpu: int = 2048, memory_limit_mib: int = 8192,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Network - public-subnet VPC (no NAT, zero standing cost) + free S3 gateway endpoint.
        vpc = ec2.Vpc(
            self, "LivabilityVpc", max_azs=2, nat_gateways=0,
            subnet_configuration=[ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24)],
        )
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
        cluster = ecs.Cluster(self, "LivabilityCluster", vpc=vpc)
        task_sg = ec2.SecurityGroup(self, "LivabilityTaskSg", vpc=vpc, allow_all_outbound=True, description="Montreal livability monthly task")

        # State - durable EFS for the Dagster instance. batch.py copies $DAGSTER_HOME here in
        # a `finally`; the next task restores it - run history + dynamic partitions survive.
        file_system = efs.FileSystem(
            self, "LivabilityState", vpc=vpc, encrypted=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_policy=efs.LifecyclePolicy.AFTER_90_DAYS,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )
        file_system.connections.allow_default_port_from(task_sg)  # 2049 from the task SG
        access_point = efs.AccessPoint(  # pins posix uid/gid so the root container's writes land as a stable owner
            self, "DagsterStateAp", file_system=file_system, path="/dagster",
            create_acl=efs.Acl(owner_uid="1000", owner_gid="1000", permissions="750"),
            posix_user=efs.PosixUser(uid="1000", gid="1000"),
        )

        # Image - build the montreal Dockerfile, publish to ECR as `montreal:<version>`.
        asset = ecr_assets.DockerImageAsset(
            self, "LivabilityImage",
            directory=str(PROJECT_DIR), file="Dockerfile", platform=ecr_assets.Platform.LINUX_AMD64,
        )
        repo = ecr.Repository(self, "LivabilityRepo", repository_name=IMAGE_NAME, removal_policy=cdk.RemovalPolicy.DESTROY, empty_on_delete=True)
        ecr_deployment.ECRDeployment(
            self, "LivabilityImagePush",
            src=ecr_deployment.DockerImageName(asset.image_uri),
            dest=ecr_deployment.DockerImageName(repo.repository_uri_for_tag(IMAGE_VERSION)),
        )

        # Alerts - SNS topic for data-quality ERROR failures (confirm subscription once via email).
        alert_topic = sns.Topic(self, "LivabilityAlerts")
        if alert_email: alert_topic.add_subscription(subscriptions.EmailSubscription(alert_email))

        # Task definition - image + S3/SNS/EFS grants + CloudWatch logs.
        task_definition = ecs.FargateTaskDefinition(self, "LivabilityTask", cpu=cpu, memory_limit_mib=memory_limit_mib)
        bucket_arn = f"arn:aws:s3:::{data_bucket}"  # S3 grants object (/*) and bucket ARNs separately
        task_definition.task_role.add_to_principal_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], resources=[f"{bucket_arn}/*"],
        ))
        task_definition.task_role.add_to_principal_policy(iam.PolicyStatement(
            actions=["s3:ListBucket", "s3:GetBucketLocation"], resources=[bucket_arn],
        ))
        file_system.grant(task_definition.task_role, "elasticfilesystem:ClientMount", "elasticfilesystem:ClientWrite")
        alert_topic.grant_publish(task_definition.task_role)

        log_group = logs.LogGroup(
            self, "LivabilityLogs", log_group_name="/ecs/livability",
            retention=logs.RetentionDays.ONE_MONTH, removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # EFS mounts beside $DAGSTER_HOME (local disk), used only by batch.py's start/finally
        # copies - SQLite never runs on NFS.
        task_definition.add_volume(
            name="dagster-state",
            efs_volume_configuration=ecs.EfsVolumeConfiguration(
                file_system_id=file_system.file_system_id, transit_encryption="ENABLED",
                authorization_config=ecs.AuthorizationConfig(access_point_id=access_point.access_point_id, iam="ENABLED"),
            ),
        )
        container = task_definition.add_container(
            "LivabilityContainer",
            image=ecs.ContainerImage.from_ecr_repository(repo, tag=IMAGE_VERSION),
            logging=ecs.LogDrivers.aws_logs(stream_prefix="livability", log_group=log_group),
            environment={
                "S3_BUCKET": data_bucket,
                "S3_REGION": data_region,
                "ALERT_TOPIC_ARN": alert_topic.topic_arn,
                "DAGSTER_STATE_DIR": "/opt/dagster/state",
            },
        )
        container.add_mount_points(ecs.MountPoint(container_path="/opt/dagster/state", source_volume="dagster-state", read_only=False))

        # Schedule - EventBridge Scheduler -> ECS RunTask directly; the target is the same
        # RunTask call run.py fires by hand. No state machine: a one-shot batch has nothing
        # transient to retry; a missed month reruns next schedule (or run.py).
        scheduler_role = iam.Role(self, "LivabilitySchedulerRole", assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"))
        scheduler_role.add_to_policy(iam.PolicyStatement(
            actions=["ecs:RunTask"], resources=[task_definition.task_definition_arn],
            conditions={"ArnLike": {"ecs:cluster": cluster.cluster_arn}},
        ))
        scheduler_role.add_to_policy(iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[task_definition.task_role.role_arn, task_definition.execution_role.role_arn],
            conditions={"StringLike": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
        ))
        scheduler.CfnSchedule(
            self, "LivabilitySchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            state=schedule_state,  # DISABLED pauses the monthly run; run.py still works
            schedule_expression=schedule_expression,
            schedule_expression_timezone="America/Toronto",  # 1 AM Eastern (DST-aware)
            target=scheduler.CfnSchedule.TargetProperty(
                arn=cluster.cluster_arn,
                role_arn=scheduler_role.role_arn,
                ecs_parameters=scheduler.CfnSchedule.EcsParametersProperty(
                    task_definition_arn=task_definition.task_definition_arn,
                    launch_type="FARGATE", task_count=1,
                    network_configuration=scheduler.CfnSchedule.NetworkConfigurationProperty(
                        awsvpc_configuration=scheduler.CfnSchedule.AwsVpcConfigurationProperty(
                            subnets=[s.subnet_id for s in vpc.public_subnets],
                            security_groups=[task_sg.security_group_id],
                            assign_public_ip="ENABLED",
                        )
                    ),
                ),
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(maximum_retry_attempts=0),
            ),
        )

        # Outputs - consumed by run.py to fire an on-demand ecs:RunTask.
        cdk.CfnOutput(self, "ClusterArn", value=cluster.cluster_arn)
        cdk.CfnOutput(self, "TaskDefinitionArn", value=task_definition.task_definition_arn)
        cdk.CfnOutput(self, "Subnets", value=",".join(s.subnet_id for s in vpc.public_subnets))
        cdk.CfnOutput(self, "SecurityGroupId", value=task_sg.security_group_id)


app = cdk.App()
ctx = app.node.try_get_context  # override any default with `cdk deploy -c key=value`

LivabilityStack(
    app,
    "LivabilityStack",
    data_bucket=ctx("data_bucket") or "montreal-livability",
    data_region=ctx("data_region") or "ca-central-1",
    schedule_expression=ctx("schedule_expression") or "cron(0 1 1 * ? *)",
    schedule_state=ctx("schedule_state") or "ENABLED",
    alert_email=ctx("alert_email") or "volodin.kostia@gmail.com",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region="ca-central-1",
    ),
)
app.synth()

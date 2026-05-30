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
    aws_iam as iam,
    aws_logs as logs,
    aws_scheduler as scheduler,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_stepfunctions as sfn,
)
from constructs import Construct

# Docker build context (workspace root is parents[2] from this file).
PROJECT_DIR = Path(__file__).resolve().parents[2] / "projects" / "montreal"
assert PROJECT_DIR.is_dir(), f"montreal project not found at {PROJECT_DIR}"


def _project_meta() -> tuple[str, str]:
    """(name, version) from the montreal pyproject — used as the ECR repo + tag."""
    text = (PROJECT_DIR / "pyproject.toml").read_text()
    return (
        re.search(r'(?m)^name\s*=\s*"([^"]+)"', text)[1],
        re.search(r'(?m)^version\s*=\s*"([^"]+)"', text)[1],
    )
IMAGE_NAME, IMAGE_VERSION = _project_meta()


class LivabilityStack(cdk.Stack):
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

        vpc, cluster = self._network()
        image = self._image()
        alert_topic = self._alerts(alert_email)
        task_definition = self._task_definition(image, data_bucket, data_region, alert_topic, cpu=cpu, memory_limit_mib=memory_limit_mib)
        state_machine = self._state_machine(vpc, cluster, task_definition)
        self._schedule(state_machine, schedule_expression, schedule_state)

        # Consumed by run.py to fire an on-demand execution.
        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)


    def _network(self) -> tuple[ec2.Vpc, ecs.Cluster]:
        """Public-subnet VPC (no NAT) + free S3 gateway endpoint, and the cluster."""

        vpc = ec2.Vpc(
            self, "LivabilityVpc", max_azs=2, nat_gateways=0,
            subnet_configuration=[ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24)],
        )
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)
        cluster = ecs.Cluster(self, "LivabilityCluster", vpc=vpc, enable_fargate_capacity_providers=True)
        return vpc, cluster


    def _alerts(self, alert_email: str) -> sns.Topic:
        """SNS topic for data-quality ERROR failures; subscribes alert_email (confirm once via email)."""

        topic = sns.Topic(self, "LivabilityAlerts")
        if alert_email: topic.add_subscription(subscriptions.EmailSubscription(alert_email))
        return topic


    def _image(self) -> ecs.ContainerImage:
        """Build and publish the montreal image to ECR."""

        asset = ecr_assets.DockerImageAsset(
            self, "LivabilityImage",
            directory=str(PROJECT_DIR),
            file="Dockerfile",
            platform=ecr_assets.Platform.LINUX_AMD64,
        )
        repo = ecr.Repository(
            self, "LivabilityRepo",
            repository_name=IMAGE_NAME,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            empty_on_delete=True,
        )
        ecr_deployment.ECRDeployment(
            self, "LivabilityImagePush",
            src=ecr_deployment.DockerImageName(asset.image_uri),
            dest=ecr_deployment.DockerImageName(repo.repository_uri_for_tag(IMAGE_VERSION)),
        )
        return ecs.ContainerImage.from_ecr_repository(repo, tag=IMAGE_VERSION)

    def _task_definition(self, image: ecs.ContainerImage, data_bucket: str, data_region: str, alert_topic: sns.Topic, *, cpu: int, memory_limit_mib: int) -> ecs.FargateTaskDefinition:
        """Fargate task def: the montreal image, lakehouse + SNS IAM, and CloudWatch logs."""

        task_definition = ecs.FargateTaskDefinition(
            self,
            "LivabilityTask",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
        )

        # S3 needs object ARNs (/*) and bucket ARNs granted separately.
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

        log_group = logs.LogGroup(
            self, "LivabilityLogs",
            log_group_name="/ecs/livability",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )
        task_definition.add_container(
            "LivabilityContainer",
            image=image,
            logging=ecs.LogDrivers.aws_logs(stream_prefix="livability", log_group=log_group),
            environment={
                "S3_BUCKET": data_bucket,
                "S3_REGION": data_region,
                "ALERT_TOPIC_ARN": alert_topic.topic_arn,
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
            },
        )
        alert_topic.grant_publish(task_definition.task_role)
        return task_definition

    def _state_machine(self, vpc: ec2.Vpc, cluster: ecs.Cluster, task_definition: ecs.FargateTaskDefinition) -> sfn.StateMachine:
        """ecs:runTask.sync on Fargate Spot, wrapped so a Spot interruption (or any
        non-zero exit) relaunches the task until a run completes clean. Each relaunch
        is cheap/idempotent — the S3 cache skips already-materialized work."""
        task_sg = ec2.SecurityGroup(self, "LivabilityTaskSg", vpc=vpc, allow_all_outbound=True, description="Montreal livability monthly task")

        run_task = sfn.CustomState(
            self,
            "RunLivability",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::ecs:runTask.sync",
                "Parameters": {
                    "Cluster": cluster.cluster_arn,
                    "TaskDefinition": task_definition.task_definition_arn,
                    "CapacityProviderStrategy": [{"CapacityProvider": "FARGATE_SPOT", "Weight": 1}],
                    "NetworkConfiguration": {
                        "AwsvpcConfiguration": {
                            "Subnets": [s.subnet_id for s in vpc.public_subnets],
                            "SecurityGroups": [task_sg.security_group_id],
                            "AssignPublicIp": "ENABLED",
                        }
                    },
                },
                # Spot interruption surfaces as States.TaskFailed; relaunch up to 10x.
                # 10 straight Spot kills in one monthly run is effectively nil.
                "Retry": [{
                    "ErrorEquals": ["States.TaskFailed", "States.Timeout"],
                    "IntervalSeconds": 30, "MaxAttempts": 10, "BackoffRate": 1.5,
                }],
            },
        )

        state_machine = sfn.StateMachine(
            self,
            "LivabilityStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(run_task),
            timeout=cdk.Duration.hours(6),
        )

        # CustomState gets none of the auto-IAM the L2 EcsRunTask would add, so grant it
        # by hand: RunTask + the lifecycle/PassRole perms .sync needs, plus the managed
        # EventBridge rule it uses to wait for the task to finish.
        role = state_machine.role
        role.add_to_principal_policy(iam.PolicyStatement(
            actions=["ecs:RunTask"], resources=[task_definition.task_definition_arn],
            conditions={"ArnLike": {"ecs:cluster": cluster.cluster_arn}},
        ))
        role.add_to_principal_policy(iam.PolicyStatement(
            actions=["ecs:StopTask", "ecs:DescribeTasks"], resources=["*"],
        ))
        role.add_to_principal_policy(iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[task_definition.task_role.role_arn, task_definition.execution_role.role_arn],
            conditions={"StringLike": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
        ))
        role.add_to_principal_policy(iam.PolicyStatement(
            actions=["events:PutTargets", "events:PutRule", "events:DescribeRule"],
            resources=[f"arn:aws:events:{self.region}:{self.account}:rule/StepFunctionsGetEventsForECSTaskRule"],
        ))
        return state_machine

    def _schedule(self, state_machine: sfn.StateMachine, schedule_expression: str, schedule_state: str) -> None:
        """Monthly EventBridge Scheduler -> Step Functions StartExecution (the state
        machine owns task launch + Spot retries)."""
        scheduler_role = iam.Role(self, "LivabilitySchedulerRole", assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"))
        scheduler_role.add_to_policy(iam.PolicyStatement(
            actions=["states:StartExecution"], resources=[state_machine.state_machine_arn],
        ))

        scheduler.CfnSchedule(
            self,
            "LivabilitySchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            state=schedule_state,  # DISABLED pauses the monthly run; run.py still works
            schedule_expression=schedule_expression,
            schedule_expression_timezone="UTC",
            target=scheduler.CfnSchedule.TargetProperty(
                arn=state_machine.state_machine_arn,
                role_arn=scheduler_role.role_arn,
                # No scheduler-level retry: the state machine handles relaunches.
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(maximum_retry_attempts=0),
            ),
        )


app = cdk.App()


def _setting(key: str, env_var: str, default: str) -> str:
    """CDK context (-c key=value) wins, then an env var, then the default."""
    return app.node.try_get_context(key) or os.environ.get(env_var) or default


LivabilityStack(
    app,
    "LivabilityStack",
    data_bucket=_setting("data_bucket", "S3_BUCKET", "montreal-livability"),
    data_region=_setting("data_region", "S3_REGION", "ca-central-1"),
    schedule_expression=_setting("schedule_expression", "SCHEDULE_EXPRESSION", "cron(0 1 1 * ? *)"),
    schedule_state=_setting("schedule_state", "SCHEDULE_STATE", "ENABLED"),
    alert_email=_setting("alert_email", "ALERT_EMAIL", "volodin.kostia@gmail.com"),
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region="ca-central-1",
    ),
)
app.synth()

"""Monthly Fargate batch job for the Montreal livability pipeline.

EventBridge Scheduler -> ECS RunTask (Fargate) -> one-shot `montreal` container
-> exits. No always-on infrastructure. See README.md for the full picture.
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
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_bucket: str,
        data_region: str,
        schedule_expression: str,
        schedule_state: str = "ENABLED",
        cpu: int = 2048,
        memory_limit_mib: int = 8192,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        vpc, cluster = self._network()
        image = self._image()
        task_definition = self._task_definition(image, data_bucket, data_region, cpu=cpu, memory_limit_mib=memory_limit_mib)
        task_sg = self._schedule(vpc, cluster, task_definition, schedule_expression, schedule_state)

        # Consumed by run.py to fire an on-demand task.
        cdk.CfnOutput(self, "ClusterName", value=cluster.cluster_name)
        cdk.CfnOutput(self, "TaskDefinitionArn", value=task_definition.task_definition_arn)
        cdk.CfnOutput(self, "TaskSecurityGroupId", value=task_sg.security_group_id)
        cdk.CfnOutput(self, "PublicSubnetIds", value=",".join(s.subnet_id for s in vpc.public_subnets),)


    def _network(self) -> tuple[ec2.Vpc, ecs.Cluster]:
        """Public-subnet VPC (no NAT) + free S3 gateway endpoint, and the cluster."""

        vpc = ec2.Vpc(
            self, "LivabilityVpc", max_azs=2, nat_gateways=0,
            subnet_configuration=[ec2.SubnetConfiguration(name="public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24)],
        )
        vpc.add_gateway_endpoint("S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3)

        cluster = ecs.Cluster(self, "LivabilityCluster", vpc=vpc)

        return vpc, cluster


    def _image(self) -> ecs.ContainerImage:
        """Build the montreal image, then publish it to a named ECR repo as
        `{IMAGE_NAME}:{IMAGE_VERSION}` so it's trackable (vs. CDK's content-hash tag)."""
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
            empty_on_delete=True,  # cdk destroy clears images too
        )
        ecr_deployment.ECRDeployment(
            self, "LivabilityImagePush",
            src=ecr_deployment.DockerImageName(asset.image_uri),
            dest=ecr_deployment.DockerImageName(repo.repository_uri_for_tag(IMAGE_VERSION)),
        )
        return ecs.ContainerImage.from_ecr_repository(repo, tag=IMAGE_VERSION)

    def _task_definition(self, image: ecs.ContainerImage, data_bucket: str, data_region: str, *, cpu: int, memory_limit_mib: int) -> ecs.FargateTaskDefinition:
        """Fargate task def: the montreal image, lakehouse IAM, and CloudWatch logs."""

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

        task_definition.add_container(
            "LivabilityContainer",
            image=image,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="livability",
                log_retention=logs.RetentionDays.ONE_MONTH,
            ),
            environment={
                "S3_BUCKET": data_bucket,
                "S3_REGION": data_region,
                # Blank keys -> boto3 falls back to the task role (no static secrets).
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
        schedule_state: str,
    ) -> ec2.SecurityGroup:
        """Monthly EventBridge Scheduler -> ECS RunTask. Returns the task's SG."""
        # Scheduler needs RunTask + PassRole (easy to forget) on the task roles.
        scheduler_role = iam.Role(
            self,
            "LivabilitySchedulerRole",
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
                conditions={"StringLike": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
            )
        )

        task_sg = ec2.SecurityGroup(self, "LivabilityTaskSg", vpc=vpc, allow_all_outbound=True, description="Montreal livability monthly task")

        scheduler.CfnSchedule(
            self,
            "LivabilitySchedule",
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(mode="OFF"),
            state=schedule_state,  # DISABLED pauses the monthly run; run.py still works
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
                # No retries: a monthly batch shouldn't duplicate-run.
                retry_policy=scheduler.CfnSchedule.RetryPolicyProperty(maximum_retry_attempts=0),
            ),
        )
        return task_sg


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
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region="ca-central-1",
    ),
)
app.synth()

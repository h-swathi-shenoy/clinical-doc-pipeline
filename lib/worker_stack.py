"""CDK stack: VPC, ECS cluster, and the two Fargate worker services.

Both services run from the SAME container image (see workers/Dockerfile);
only the ECS `command` override differs per service, selecting which
worker script (`classification_worker.py` vs `extraction_worker.py`) runs.
This keeps exactly one image to build/scan/promote for both workloads.

IAM is least-privilege and split per service:
  * classification-worker's task role can only consume the
    classification-queue (+ its DLQ redrive is handled by SQS itself,
    no extra IAM needed) and write to the results table.
  * extraction-worker's task role can only consume the extraction-queue,
    write to the results table, AND send (only send, not consume) to the
    classification-queue -- this is the pipeline hand-off: extraction is
    the entry point, classification is the dependent downstream stage, and
    a message only ever reaches classification-queue after extraction's own
    result is durably persisted.
  * Both roles get `bedrock:InvokeModel` scoped to the specific Nova model
    ARN, nothing broader.

Auto scaling is queue-depth driven (`ApproximateNumberOfMessagesVisible`)
so each service scales independently based on its own backlog, from 1 up
to 10 tasks.
"""

from __future__ import annotations

from aws_cdk import Duration, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

MIN_TASKS = 1
MAX_TASKS = 10

# Scale-out target: keep roughly this many visible messages "per task" in
# steady state; ECS Application Auto Scaling adjusts task count to try to
# hold the metric near this value.
TARGET_MESSAGES_PER_TASK = 5

BEDROCK_MODEL_ID = "amazon.nova-lite-v1:0"


class WorkerStack(Stack):
    """Provisions networking, the ECS cluster, and both Fargate services."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        classification_queue: sqs.Queue,
        extraction_queue: sqs.Queue,
        results_table: dynamodb.Table,
        image_asset_path: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Small, cost-conscious VPC: no NAT gateways -- workers only need
        # outbound access to AWS service endpoints, provided here via
        # interface/gateway VPC endpoints so tasks can run in private
        # isolated subnets without any public internet route.
        vpc = ec2.Vpc(
            self,
            "WorkerVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
        )

        # Gateway endpoint for DynamoDB (no hourly/data charge).
        vpc.add_gateway_endpoint(
            "DynamoDbEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )

        # Gateway endpoint for S3 (no hourly/data charge) -- required
        # alongside the ECR_DOCKER interface endpoint below, since ECR
        # stores image layers in S3 and the Docker daemon fetches them via
        # this route when there is no NAT/internet path.
        vpc.add_gateway_endpoint(
            "S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3
        )

        # Interface endpoints for SQS, Bedrock runtime, CloudWatch Logs, and
        # ECR (both the API for auth and the Docker registry for image/layer
        # pulls) so tasks in isolated subnets can reach these services
        # privately -- without these, ECS cannot authenticate to or pull
        # images from ECR since there is no NAT gateway/public route.
        for endpoint_id, service in (
            ("SqsEndpoint", ec2.InterfaceVpcEndpointAwsService.SQS),
            ("BedrockRuntimeEndpoint", ec2.InterfaceVpcEndpointAwsService.BEDROCK_RUNTIME),
            ("LogsEndpoint", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS),
            ("EcrApiEndpoint", ec2.InterfaceVpcEndpointAwsService.ECR),
            ("EcrDkrEndpoint", ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
        ):
            vpc.add_interface_endpoint(endpoint_id, service=service)

        cluster = ecs.Cluster(self, "WorkerCluster", vpc=vpc, container_insights_v2=ecs.ContainerInsights.ENABLED)

        bedrock_model_arn = (
            f"arn:aws:bedrock:{self.region}::foundation-model/{BEDROCK_MODEL_ID}"
        )

        image_asset = ecs.ContainerImage.from_asset(image_asset_path)

        self._build_worker_service(
            cluster=cluster,
            service_id="ClassificationWorker",
            command=["python", "classification_worker.py"],
            image=image_asset,
            queue=classification_queue,
            queue_env_var="CLASSIFICATION_QUEUE_URL",
            results_table=results_table,
            bedrock_model_arn=bedrock_model_arn,
            log_group_name="/ecs/classification-worker",
        )

        self._build_worker_service(
            cluster=cluster,
            service_id="ExtractionWorker",
            command=["python", "extraction_worker.py"],
            image=image_asset,
            queue=extraction_queue,
            queue_env_var="EXTRACTION_QUEUE_URL",
            results_table=results_table,
            bedrock_model_arn=bedrock_model_arn,
            log_group_name="/ecs/extraction-worker",
            # extraction-worker is the pipeline entry point: after it
            # durably persists its own result it hands the document off to
            # classification-queue, so it needs send-only rights there and
            # the queue's URL in its environment.
            downstream_send_queue=classification_queue,
            downstream_queue_env_var="CLASSIFICATION_QUEUE_URL",
        )

    def _build_worker_service(
        self,
        cluster: ecs.Cluster,
        service_id: str,
        command: list[str],
        image: ecs.ContainerImage,
        queue: sqs.Queue,
        queue_env_var: str,
        results_table: dynamodb.Table,
        bedrock_model_arn: str,
        log_group_name: str,
        downstream_send_queue: sqs.Queue | None = None,
        downstream_queue_env_var: str | None = None,
    ) -> None:
        """Create one Fargate service (task def, IAM, service, autoscaling)."""

        task_role = iam.Role(
            self,
            f"{service_id}TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description=f"Least-privilege execution role for {service_id}",
        )

        # Least privilege: consume rights ONLY on this worker's own queue.
        # (No SendMessage/DLQ access on the CONSUMED queue -- DLQ redrive is
        # an operator action, not something the worker itself needs.)
        queue.grant_consume_messages(task_role)

        # Least privilege: write-only on the results table (no read-back
        # needed since workers never re-read prior results).
        results_table.grant_write_data(task_role)

        # Bedrock invoke scoped to the specific pinned model ARN only.
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=[bedrock_model_arn],
            )
        )

        environment = {
            queue_env_var: queue.queue_url,
            "RESULTS_TABLE_NAME": results_table.table_name,
            "BEDROCK_MODEL_ID": BEDROCK_MODEL_ID,
        }

        if downstream_send_queue is not None:
            # Send-only rights on the DOWNSTREAM queue: this service may
            # hand work off to the next pipeline stage, but must never
            # consume from it -- that would blur the sequential dependency
            # this stack is modeling.
            downstream_send_queue.grant_send_messages(task_role)
            environment[downstream_queue_env_var] = downstream_send_queue.queue_url

        log_group = logs.LogGroup(
            self,
            f"{service_id}LogGroup",
            log_group_name=log_group_name,
            retention=logs.RetentionDays.ONE_MONTH,
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            f"{service_id}TaskDef",
            cpu=512,
            memory_limit_mib=1024,
            task_role=task_role,
        )

        task_definition.add_container(
            f"{service_id}Container",
            image=image,
            command=command,
            environment=environment,
            logging=ecs.LogDrivers.aws_logs(stream_prefix=service_id, log_group=log_group),
        )

        service = ecs.FargateService(
            self,
            f"{service_id}Service",
            cluster=cluster,
            task_definition=task_definition,
            desired_count=MIN_TASKS,
            min_healthy_percent=100,
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_ISOLATED),
            # No load balancer / no public IP: this is a pure queue
            # consumer, there is intentionally no REST API surface.
            assign_public_ip=False,
        )

        scaling = service.auto_scale_task_count(min_capacity=MIN_TASKS, max_capacity=MAX_TASKS)
        scaling.scale_on_metric(
            f"{service_id}QueueDepthScaling",
            metric=queue.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1)
            ),
            scaling_steps=[
                # No messages visible -> scale toward the floor.
                {"upper": 0, "change": -1},
                # Roughly `TARGET_MESSAGES_PER_TASK` messages per task ->
                # hold steady.
                {"lower": 1, "upper": TARGET_MESSAGES_PER_TASK, "change": 0},
                # Backlog growing -> scale out.
                {"lower": TARGET_MESSAGES_PER_TASK + 1, "change": +1},
            ],
            cooldown=Duration.minutes(2),
        )

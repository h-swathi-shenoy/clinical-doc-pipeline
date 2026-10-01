"""CDK stack: DynamoDB results table.

GxP note: `doc_id` is the sole partition key. Both workers write results
via `put_item` keyed on `doc_id` (see workers/common/db_client.py), so a
message redelivered by SQS after a partial failure simply overwrites the
same item -- idempotent by construction, with no duplicate records ever
created. Billing mode is on-demand (PAY_PER_REQUEST) since traffic is
bursty and driven by queue depth rather than steady load.
"""

from __future__ import annotations

from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct

TABLE_NAME = "clinical-results"


class DataStack(Stack):
    """Provisions the `clinical-results` DynamoDB table."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.results_table = dynamodb.Table(
            self,
            "ClinicalResultsTable",
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(
                name="doc_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            # RETAIN: this is a reference implementation, but in a real
            # regulated environment, clinical result data must never be
            # deleted as a side effect of `cdk destroy` / stack teardown.
            removal_policy=RemovalPolicy.RETAIN,
        )

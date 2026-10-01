"""CDK stack: SQS queues + Dead Letter Queues for the pipeline.

GxP note: every queue has a companion DLQ. `maxReceiveCount=3` means a
message that fails processing three times (worker crashed, malformed
document, transient Bedrock error, etc.) is automatically redirected to the
DLQ instead of being retried forever or silently dropped -- this gives QA /
support a durable, inspectable record of every document that could not be
processed automatically, which is required for a defensible audit trail in
a regulated pipeline. The 14-day retention on the DLQ maximizes the window
available for investigation and manual redrive.
"""

from __future__ import annotations

from aws_cdk import Duration, Stack
from aws_cdk import aws_sqs as sqs
from constructs import Construct

# Must comfortably exceed the worst-case Bedrock inference latency for a
# single document so that an in-flight message never becomes visible again
# (and gets picked up by a second worker) while still being processed.
VISIBILITY_TIMEOUT = Duration.minutes(5)

DLQ_RETENTION = Duration.days(14)
MAX_RECEIVE_COUNT = 3


class QueueStack(Stack):
    """Provisions the classification and extraction queues and their DLQs."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.classification_dlq = sqs.Queue(
            self,
            "ClassificationDLQ",
            queue_name="classification-queue-dlq",
            retention_period=DLQ_RETENTION,
        )
        self.classification_queue = sqs.Queue(
            self,
            "ClassificationQueue",
            queue_name="classification-queue",
            visibility_timeout=VISIBILITY_TIMEOUT,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=MAX_RECEIVE_COUNT,
                queue=self.classification_dlq,
            ),
        )

        self.extraction_dlq = sqs.Queue(
            self,
            "ExtractionDLQ",
            queue_name="extraction-queue-dlq",
            retention_period=DLQ_RETENTION,
        )
        self.extraction_queue = sqs.Queue(
            self,
            "ExtractionQueue",
            queue_name="extraction-queue",
            visibility_timeout=VISIBILITY_TIMEOUT,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=MAX_RECEIVE_COUNT,
                queue=self.extraction_dlq,
            ),
        )

"""SQS helper for GxP-style, at-least-once, delete-after-success processing.

GxP note: messages are ONLY deleted from the queue after the business logic
(classification / extraction) and the audit log write have both succeeded.
This guarantees that a crash mid-processing leaves the message visible again
after the visibility timeout expires, so it is retried automatically. After
`maxReceiveCount` failed attempts SQS redrives the message to its Dead Letter
Queue (DLQ) for human/QA review instead of silently dropping it -- this is
the core "nothing gets lost" guarantee required for regulated document
processing.

Pipeline chaining note: the pipeline stages are DEPENDENT, not independent.
extraction-worker is the entry point (consumes `extraction-queue`); only
after it has durably written its result to DynamoDB and logged the audit
event does it call `send_message` to push the same `doc_id`/`document_text`
onto `classification-queue`, which classification-worker then consumes. This
keeps the "only advance on proven-durable success" guarantee end-to-end
across both stages, not just within one.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterator

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

# Long polling: reduces empty-receive cost and API call volume compared to
# short polling. 20 seconds is the SQS maximum.
WAIT_TIME_SECONDS = 20

# Visibility timeout is set on the queue itself (see lib/queue-stack.py) to
# 5 minutes, comfortably longer than the expected worst-case Bedrock
# inference latency, so an in-flight message never becomes visible again
# (and reprocessed by a second worker) while it is still legitimately being
# handled.
MAX_MESSAGES_PER_POLL = 1


class QueueClient:
    """Thin wrapper around a single SQS queue for a single worker."""

    def __init__(self, queue_url: str, region_name: str | None = None) -> None:
        """Create a client bound to one queue.

        Args:
            queue_url: Full URL of the SQS queue to consume from.
            region_name: Optional AWS region override (defaults to the
                region configured in the container's environment).
        """
        self.queue_url = queue_url
        # Modest retry config: SQS API calls are idempotent (receive/delete)
        # so we can safely retry on transient network errors.
        self._sqs = boto3.client(
            "sqs",
            region_name=region_name,
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    def poll(self) -> Iterator[dict[str, Any]]:
        """Long-poll the queue and yield raw SQS message dicts.

        Each yielded message still needs to be explicitly deleted by the
        caller via `delete_message` once processing succeeds. Never deletes
        automatically -- that is the caller's responsibility, and only on
        success.
        """
        response = self._sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=MAX_MESSAGES_PER_POLL,
            WaitTimeSeconds=WAIT_TIME_SECONDS,
            AttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        for message in messages:
            yield message

    def delete_message(self, receipt_handle: str) -> None:
        """Acknowledge successful processing by deleting the message.

        Only ever called AFTER the result has been durably written to
        DynamoDB and the audit trail entry has been logged. This ordering
        (do the work -> persist result -> audit -> THEN ack) is what makes
        failures safe: a crash before this point leaves the message for a
        retry instead of silently losing it.
        """
        self._sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)

    def receive_count(self, message: dict[str, Any]) -> int:
        """Return how many times this message has been received so far."""
        attrs = message.get("Attributes", {})
        return int(attrs.get("ApproximateReceiveCount", "1"))

    def send_message(self, body: dict[str, Any]) -> None:
        """Publish a message to this queue (used for stage-to-stage chaining).

        Only ever called AFTER the upstream stage's own result has been
        durably written to DynamoDB and audited -- so the downstream stage
        is only ever notified of work that is already safely recorded.
        """
        self._sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps(body))

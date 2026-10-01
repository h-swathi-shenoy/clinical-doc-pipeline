"""classification-worker: classifies a synthetic medical document.

This is the DOWNSTREAM stage of the pipeline (not an independent entry
point). It only ever receives a message because extraction-worker already
processed the same `doc_id`, durably wrote its result to DynamoDB, and then
explicitly pushed it onto `classification-queue` -- see
`extraction_worker.py` for that hand-off. classification-worker then
long-polls `classification-queue`, invokes Bedrock (Nova) to classify the
document into one of a fixed label set, writes the result to DynamoDB
(same `doc_id`, so it merges into the item extraction already created),
emits an audit event, and only THEN deletes the message from the queue.

GxP-relevant behaviors (see inline comments and `common/` modules for more):
  * delete-after-success -> automatic retry on failure via SQS visibility
    timeout; permanent failures land in the DLQ after 3 attempts.
  * doc_id is the DynamoDB key -> reprocessing is idempotent.
  * every message produces exactly one audit log line.
  * graceful SIGTERM handling -> ECS can stop tasks (e.g. during scale-in
    or deployment) without truncating in-flight work.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import types
from typing import Any

from common.audit import hash_input, log_audit_event
from common.db_client import ResultsTableClient
from common.llm_client import LLMClient
from common.queue_client import QueueClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("classification-worker")

WORKER_NAME = "classification-worker"

VALID_LABELS = [
    "Lab Report",
    "Discharge Summary",
    "Radiology Note",
    "Prescription",
    "Progress Note",
]

SYSTEM_PROMPT = (
    "You are a clinical document classifier. Classify the given synthetic "
    "medical document into exactly one of the following categories: "
    f"{', '.join(VALID_LABELS)}. "
    'Respond with ONLY a compact JSON object of the form {"label": "<category>"} '
    "and nothing else."
)

# Module-level flag flipped by the SIGTERM handler; checked between messages
# so the worker finishes whatever it is currently processing before exiting.
_shutdown_requested = False


def _handle_sigterm(signum: int, frame: types.FrameType | None) -> None:
    """Mark shutdown requested; let the current message finish processing."""
    global _shutdown_requested
    logger.info("Received signal %s, finishing current message then exiting", signum)
    _shutdown_requested = True


def _parse_label(model_output: str) -> str:
    """Extract and validate the classification label from model output."""
    try:
        parsed: dict[str, Any] = json.loads(model_output)
        label = str(parsed["label"]).strip()
    except (json.JSONDecodeError, KeyError):
        # Fall back to substring match if the model didn't return clean JSON.
        label = next((cand for cand in VALID_LABELS if cand.lower() in model_output.lower()), "")
    if label not in VALID_LABELS:
        raise ValueError(f"Model returned unrecognized label: {model_output!r}")
    return label


def process_message(
    body: dict[str, Any],
    llm: LLMClient,
    results_table: ResultsTableClient,
) -> None:
    """Classify one document and persist the result + audit event.

    Args:
        body: Parsed message body with at least `doc_id` and `document_text`.
        llm: Shared Bedrock client wrapper.
        results_table: Shared DynamoDB results client.
    """
    doc_id = body["doc_id"]
    document_text = body["document_text"]
    input_hash = hash_input(document_text)

    model_output = llm.invoke(system_prompt=SYSTEM_PROMPT, user_prompt=document_text)
    label = _parse_label(model_output)

    # Idempotent write: safe even if this message is later redelivered.
    results_table.put_result(
        doc_id=doc_id,
        worker=WORKER_NAME,
        payload={"classification": label, "model_id": llm.model_id},
    )

    # Audit trail is written BEFORE the message is deleted, so a crash
    # between the two would simply cause a harmless re-audit on retry.
    log_audit_event(
        doc_id=doc_id,
        worker=WORKER_NAME,
        model_id=llm.model_id,
        input_hash=input_hash,
        output_summary=f"classification={label}",
    )
    logger.info("doc_id=%s classified as %s", doc_id, label)


def main() -> None:
    """Long-poll loop: receive, process, delete-on-success, repeat."""
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    queue_url = os.environ["CLASSIFICATION_QUEUE_URL"]
    table_name = os.environ["RESULTS_TABLE_NAME"]

    queue = QueueClient(queue_url=queue_url)
    llm = LLMClient()
    results_table = ResultsTableClient(table_name=table_name)

    logger.info("classification-worker started, polling %s", queue_url)
    while not _shutdown_requested:
        for message in queue.poll():
            try:
                body = json.loads(message["Body"])
                process_message(body, llm, results_table)
            except Exception:  # noqa: BLE001 - must not crash the loop
                # Do NOT delete the message: letting the visibility timeout
                # expire triggers an automatic retry, and after
                # maxReceiveCount attempts SQS moves it to the DLQ for
                # review instead of losing it.
                logger.exception(
                    "Failed to process message id=%s, leaving for retry/DLQ",
                    message.get("MessageId"),
                )
                continue
            queue.delete_message(message["ReceiptHandle"])

            if _shutdown_requested:
                break

    logger.info("classification-worker shutting down cleanly")


if __name__ == "__main__":
    main()

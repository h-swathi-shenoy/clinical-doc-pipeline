"""extraction-worker: extracts structured clinical entities as JSON.

This is the ENTRY POINT of the pipeline. It long-polls `extraction-queue`,
invokes Bedrock (Nova) to extract a fixed schema of clinical entities from a
synthetic document, writes the result to DynamoDB, emits an audit event,
and only THEN (a) deletes the message from `extraction-queue` and (b) hands
the document off to `classification-queue` so classification-worker can run
next. The pipeline stages are DEPENDENT/sequential, not independent:
extraction always runs first; classification only ever sees a document
after its extraction result is durably persisted. See
`classification_worker.py` for a fuller explanation of the GxP-relevant
delete-after-success / idempotency / audit-trail pattern shared by both
workers.
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
logger = logging.getLogger("extraction-worker")

WORKER_NAME = "extraction-worker"

REQUIRED_FIELDS = [
    "patient_id",
    "patient_name",
    "date_of_service",
    "diagnosis",
    "medications",
    "provider_name",
]

SYSTEM_PROMPT = (
    "You are a clinical information extraction system operating ONLY on "
    "synthetic, non-real test documents. Extract the following structured "
    "entities as a single compact JSON object with EXACTLY these keys: "
    "patient_id (string), patient_name (string), date_of_service "
    "(string, YYYY-MM-DD if determinable), diagnosis (string), "
    "medications (array of objects with keys name, dosage, frequency), "
    "provider_name (string). If a field cannot be determined, use an empty "
    "string (or empty array for medications). Respond with ONLY the JSON "
    "object and nothing else."
)

# Flipped by the SIGTERM handler; checked between messages so in-flight work
# always finishes before the process exits (clean ECS task stop).
_shutdown_requested = False


def _handle_sigterm(signum: int, frame: types.FrameType | None) -> None:
    """Mark shutdown requested; let the current message finish processing."""
    global _shutdown_requested
    logger.info("Received signal %s, finishing current message then exiting", signum)
    _shutdown_requested = True


def _parse_entities(model_output: str) -> dict[str, Any]:
    """Parse and validate the extracted-entities JSON from model output."""
    parsed: dict[str, Any] = json.loads(model_output)
    missing = [field for field in REQUIRED_FIELDS if field not in parsed]
    if missing:
        raise ValueError(f"Model output missing required fields {missing}: {model_output!r}")
    return parsed


def process_message(
    body: dict[str, Any],
    llm: LLMClient,
    results_table: ResultsTableClient,
    classification_queue: QueueClient,
) -> None:
    """Extract structured entities for one document, persist + audit, then
    hand the document off to classification-queue.

    Args:
        body: Parsed message body with at least `doc_id` and `document_text`.
        llm: Shared Bedrock client wrapper.
        results_table: Shared DynamoDB results client.
        classification_queue: Client for the DOWNSTREAM classification-queue.
            Only sent to after this stage's result is durably persisted, so
            classification never starts on a document whose extraction
            result isn't already safely recorded.
    """
    doc_id = body["doc_id"]
    document_text = body["document_text"]
    input_hash = hash_input(document_text)

    model_output = llm.invoke(
        system_prompt=SYSTEM_PROMPT, user_prompt=document_text, max_tokens=1536
    )
    entities = _parse_entities(model_output)

    # Idempotent write keyed on doc_id: a redelivered message safely
    # overwrites the same DynamoDB item instead of creating a duplicate.
    results_table.put_result(
        doc_id=doc_id,
        worker=WORKER_NAME,
        payload={"entities": entities, "model_id": llm.model_id},
    )

    # Non-PHI summary only (diagnosis category presence, med count) -- the
    # audit trail must never itself become a repository of extracted PHI.
    output_summary = f"diagnosis_present={bool(entities.get('diagnosis'))} " \
        f"medication_count={len(entities.get('medications') or [])}"
    log_audit_event(
        doc_id=doc_id,
        worker=WORKER_NAME,
        model_id=llm.model_id,
        input_hash=input_hash,
        output_summary=output_summary,
    )
    logger.info("doc_id=%s entities extracted", doc_id)

    # Hand off to classification-queue ONLY after the extraction result is
    # durably persisted + audited. This is the dependency link that makes
    # the pipeline sequential: classification-worker never sees a doc_id
    # until its extraction has already succeeded and been recorded.
    classification_queue.send_message({"doc_id": doc_id, "document_text": document_text})
    logger.info("doc_id=%s handed off to classification-queue", doc_id)


def main() -> None:
    """Long-poll loop: receive, process, delete-on-success, repeat."""
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    queue_url = os.environ["EXTRACTION_QUEUE_URL"]
    classification_queue_url = os.environ["CLASSIFICATION_QUEUE_URL"]
    table_name = os.environ["RESULTS_TABLE_NAME"]

    queue = QueueClient(queue_url=queue_url)
    classification_queue = QueueClient(queue_url=classification_queue_url)
    llm = LLMClient()
    results_table = ResultsTableClient(table_name=table_name)

    logger.info("extraction-worker started, polling %s", queue_url)
    while not _shutdown_requested:
        for message in queue.poll():
            try:
                body = json.loads(message["Body"])
                process_message(body, llm, results_table, classification_queue)
            except Exception:  # noqa: BLE001 - must not crash the loop
                # Leave the message un-deleted: SQS visibility timeout
                # expiry triggers a retry, and repeated failures (up to
                # maxReceiveCount) redrive to the DLQ for QA review.
                logger.exception(
                    "Failed to process message id=%s, leaving for retry/DLQ",
                    message.get("MessageId"),
                )
                continue
            queue.delete_message(message["ReceiptHandle"])

            if _shutdown_requested:
                break

    logger.info("extraction-worker shutting down cleanly")


if __name__ == "__main__":
    main()

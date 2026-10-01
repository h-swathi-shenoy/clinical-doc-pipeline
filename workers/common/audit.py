"""Structured audit logging for regulated document processing.

Every processed message produces exactly one audit record written to
stdout (captured by the `awslogs` driver into CloudWatch Logs). The record
never contains raw patient content -- only a SHA-256 hash of the input and
a short, non-PHI summary of the output -- so the audit trail itself cannot
leak sensitive data, while still providing full traceability of
"what model, on what input, produced what output, when" for QA / compliance
review and incident investigation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

# Dedicated logger so audit records are clearly identifiable in CloudWatch
# Logs (distinct from general application log lines).
_audit_logger = logging.getLogger("audit")
_audit_logger.setLevel(logging.INFO)
if not _audit_logger.handlers:
    _handler = logging.StreamHandler(stream=sys.stdout)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _audit_logger.addHandler(_handler)
    _audit_logger.propagate = False


def hash_input(raw_text: str) -> str:
    """Return a deterministic SHA-256 hex digest of the raw document text.

    Used in the audit trail instead of the raw content so the log itself
    stays free of PHI while still letting reviewers verify (by re-hashing)
    exactly which input version was processed.
    """
    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()


def log_audit_event(
    doc_id: str,
    worker: str,
    model_id: str,
    input_hash: str,
    output_summary: str,
) -> None:
    """Emit a single structured audit record for one processed message.

    Args:
        doc_id: Stable identifier of the processed document.
        worker: Name of the worker emitting the event.
        model_id: Bedrock model identifier used for inference.
        input_hash: SHA-256 hex digest of the raw input (see `hash_input`).
        output_summary: Short, non-PHI description of the result
            (e.g. a classification label, never full extracted entities).
    """
    record: dict[str, Any] = {
        "audit": True,
        "doc_id": doc_id,
        "worker": worker,
        "model_id": model_id,
        "input_hash": input_hash,
        "output_summary": output_summary,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _audit_logger.info(json.dumps(record, sort_keys=True))

"""DynamoDB result persistence.

GxP note: `doc_id` is used as the sole partition key. Writes use
`update_item` with per-field `SET` expressions rather than `put_item`, so
each stage's write MERGES its fields into the existing item instead of
replacing it wholesale. This matters now that the pipeline is sequential:
extraction-worker writes its `entities` first, and classification-worker's
later write for the same `doc_id` must add its `classification` field
alongside `entities`, not erase it. Combined with SQS delete-after-success
semantics, this is still idempotent: if a message is redelivered (e.g. a
worker crashed after writing the result but before deleting the message),
reprocessing simply re-applies the same field-level `SET`s, an equivalent
no-op overwrite of that worker's own fields, not a duplicate record and not
a loss of the other stage's fields.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)


class ResultsTableClient:
    """Wrapper around the `clinical-results` DynamoDB table."""

    def __init__(self, table_name: str, region_name: str | None = None) -> None:
        """Create a client bound to the results table.

        Args:
            table_name: Name of the DynamoDB table (partition key `doc_id`).
            region_name: Optional AWS region override.
        """
        self.table_name = table_name
        self._table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)

    def put_result(self, doc_id: str, worker: str, payload: dict[str, Any]) -> None:
        """Idempotently merge this worker's result fields into the document's item.

        Args:
            doc_id: Stable, unique identifier for the source document. Used
                as the DynamoDB partition key so repeated writes (from
                message redelivery, or from the OTHER pipeline stage) merge
                safely instead of colliding.
            worker: Name of the worker that produced this result
                (e.g. "classification-worker" or "extraction-worker").
                Stored per-stage as `last_updated_by_<worker>` so neither
                stage's provenance is lost when the other stage writes.
            payload: JSON-serializable result payload to merge in (e.g.
                `{"entities": ...}` or `{"classification": ...}`).
        """
        fields = {
            f"last_updated_by_{worker}": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        # Expression placeholders (the `#foo`/`:foo` tokens) are parsed as
        # UpdateExpression identifiers and may only contain alphanumerics/
        # underscores -- worker names like "extraction-worker" contain a
        # hyphen, which DynamoDB rejects there. The actual stored attribute
        # name (the dict VALUE below) is unaffected and keeps the real key,
        # e.g. "last_updated_by_extraction-worker".
        placeholders = {key: key.replace("-", "_") for key in fields}
        update_expression = "SET " + ", ".join(
            f"#{placeholders[key]} = :{placeholders[key]}" for key in fields
        )
        logger.info("Merging result for doc_id=%s worker=%s", doc_id, worker)
        self._table.update_item(
            Key={"doc_id": doc_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={f"#{placeholders[key]}": key for key in fields},
            ExpressionAttributeValues={
                f":{placeholders[key]}": value for key, value in fields.items()
            },
        )

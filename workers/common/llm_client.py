"""Isolated Amazon Bedrock invocation wrapper.

Keeping ALL Bedrock API calls behind this one module means:
  * The model ID is configured in exactly one place (env var / constant),
    which matters for GxP change control -- swapping model versions is a
    single, auditable configuration change rather than a code change
    scattered across workers.
  * Unit tests can mock this module instead of the boto3 bedrock client.
  * The immutable container image can be validated once against this
    interface; the model behind it can be pinned by ID/version for
    reproducibility of validated inference runs.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

# Nova sometimes wraps its JSON response in a markdown code fence (e.g.
# ```json ... ```) even when explicitly told to respond with ONLY the JSON
# object. Both downstream workers feed this raw text straight into
# `json.loads`, so an un-stripped fence causes every message to fail
# parsing (`Expecting value: line 1 column 1`). Stripping it here, in the
# one shared Bedrock wrapper, fixes both workers at the source.
_CODE_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)

# Amazon Nova Lite via Bedrock. Pinned explicitly (no "latest" alias) so that
# a validated/qualified pipeline configuration always calls the same model
# version -- required for reproducibility in a regulated environment.
DEFAULT_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")


class LLMClient:
    """Wrapper around bedrock-runtime InvokeModel for the Nova model family."""

    def __init__(self, model_id: str | None = None, region_name: str | None = None) -> None:
        """Create a Bedrock runtime client.

        Args:
            model_id: Bedrock model identifier. Defaults to `DEFAULT_MODEL_ID`.
            region_name: Optional AWS region override.
        """
        self.model_id = model_id or DEFAULT_MODEL_ID
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=region_name,
            config=Config(retries={"max_attempts": 5, "mode": "standard"}),
        )

    def invoke(self, system_prompt: str, user_prompt: str, max_tokens: int = 1024) -> str:
        """Invoke the configured Nova model and return raw text output.

        Uses the Bedrock "Converse"-style Nova request/response envelope.

        Args:
            system_prompt: Instructions describing the task for the model.
            user_prompt: The document / content to run inference on.
            max_tokens: Upper bound on generated tokens.

        Returns:
            The model's text response, stripped of surrounding whitespace.
        """
        body = {
            "schemaVersion": "messages-v1",
            "system": [{"text": system_prompt}],
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": user_prompt}],
                }
            ],
            "inferenceConfig": {
                "maxTokens": max_tokens,
                "temperature": 0,
                "topP": 0.9,
            },
        }

        logger.info("Invoking Bedrock model=%s", self.model_id)
        response = self._client.invoke_model(
            modelId=self.model_id,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
        payload: dict[str, Any] = json.loads(response["body"].read())
        try:
            text: str = payload["output"]["message"]["content"][0]["text"]
        except (KeyError, IndexError) as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"Unexpected Bedrock response shape: {payload}") from exc
        text = text.strip()
        return _CODE_FENCE_RE.sub("", text).strip()

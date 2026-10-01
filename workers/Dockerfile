
# syntax=docker/dockerfile:1
#
# Multi-stage build for the clinical document intelligence workers.
#
# GxP note: this single immutable image is used for BOTH the
# classification-worker and extraction-worker ECS services. The entrypoint
# script differs only via the `command` override set in CDK
# (see lib/worker-stack.py), NOT via separate images. This means both
# workers are always running byte-identical, validated code -- there is
# exactly one artifact to build, scan, and promote through environments.

# ---- builder stage: build a venv with pinned dependencies -----------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build deps only in this stage; they are never present in the
# final runtime image, keeping the deployed attack surface minimal.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---- runtime stage: slim, non-root, immutable ------------------------------
FROM python:3.12-slim AS runtime

# Non-root execution: least-privilege container runtime, standard hardening
# expectation for regulated workloads.
RUN groupadd --system appgroup \
    && useradd --system --gid appgroup --create-home --home-dir /home/appuser appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY --chown=appuser:appgroup common ./common
COPY --chown=appuser:appgroup classification_worker.py extraction_worker.py ./

USER appuser

# No CMD/ENTRYPOINT here on purpose: the ECS Fargate task's container
# `command` override (set per-service in CDK) selects which worker script
# runs, off this one shared image.

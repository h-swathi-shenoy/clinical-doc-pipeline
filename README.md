# Clinical Document Intelligence Pipeline (Reference Implementation)

> **⚠️ SYNTHETIC DATA ONLY — NOT REAL PHI.** Every "patient" name, MRN,
> diagnosis, and medication in this repository (see
> `scripts/seed_dummy_data.py`) is fabricated for demonstration purposes.
> This is a compliance-aware (GxP / HIPAA-style) *reference* architecture,
> not a validated production system — treat it as a starting point for your
> own qualification process.

## What this is

An **asynchronous, queue-driven** (no REST API) pipeline that processes
synthetic medical documents using two **dependent, sequential** ECS Fargate
worker services, each backed by its own SQS queue:

1. **extraction-worker** (pipeline entry point) — extracts structured
   clinical entities as JSON: `patient_id`, `patient_name`,
   `date_of_service`, `diagnosis`, `medications: [{name, dosage,
   frequency}]`, `provider_name`.
2. **classification-worker** (downstream stage) — classifies the same
   document into one of: `Lab Report`, `Discharge Summary`,
   `Radiology Note`, `Prescription`, `Progress Note`. It only ever runs
   AFTER extraction-worker has durably persisted and audited its own
   result for that `doc_id`.

Both workers call **Amazon Bedrock** (Amazon Nova) for inference, write
results to a DynamoDB table (`clinical-results`, partition key `doc_id`),
and emit a structured, PHI-free **audit trail** entry to CloudWatch Logs for
every message processed.

## Architecture

```
                 ┌───────────────────────┐
  seed script ─▶ │   extraction-queue    │──▶ DLQ (maxReceive=3, 14d)
                 └───────────┬───────────┘
                             ▼
                 ┌───────────────────────┐        ┌─────────────────────┐
                 │   extraction-worker   │──────▶ │                     │
                 │  (ECS Fargate, 1-10)  │         │   Amazon Bedrock    │
                 │   PIPELINE ENTRY      │────────▶│   (Nova model)      │
                 └───────────┬───────────┘         └─────────────────────┘
                             │
                             ├──▶ ┌────────────────────────┐
                             │    │  clinical-results      │
                             │    │  (DynamoDB, doc_id PK)  │
                             │    └────────────────────────┘
                             │
                             ├──▶ ┌────────────────────────┐
                             │    │  CloudWatch Logs        │
                             │    │  (audit trail)          │
                             │    └────────────────────────┘
                             │
                             ▼ (only AFTER the writes above succeed)
                 ┌───────────────────────┐
                 │  classification-queue │──▶ DLQ (maxReceive=3, 14d)
                 └───────────┬───────────┘
                             ▼
                 ┌───────────────────────┐        ┌─────────────────────┐
                 │ classification-worker │──────▶ │   Amazon Bedrock    │
                 │  (ECS Fargate, 1-10)  │         │   (Nova model)      │
                 │  DEPENDENT STAGE      │────────▶│   (same as above)    │
                 └───────────┬───────────┘         └─────────────────────┘
                             │
                             ├──▶ clinical-results (same doc_id item)
                             └──▶ CloudWatch Logs (audit trail)
```

Key properties:
- **No REST API** — documents enter purely via SQS `SendMessage` (the seed
  script simulates an upstream system doing this) to `extraction-queue`,
  the pipeline's ONLY external entry point.
- **Sequential/dependent stages, not independent** — extraction always runs
  first. Only after extraction-worker durably writes its result to
  DynamoDB and logs its audit event does it explicitly `SendMessage` the
  same `doc_id`/`document_text` onto `classification-queue` (see
  `workers/extraction_worker.py`). classification-worker never sees a
  document until extraction has already succeeded for it.
- **Long-polling** (`WaitTimeSeconds=20`) to minimize empty-receive cost.
- **Delete-after-success** — a message is deleted only after its result is
  durably written to DynamoDB *and* the audit event is logged (and, for
  extraction-worker, only after the downstream hand-off succeeds too). Any
  failure leaves the message in the queue; SQS visibility timeout (5
  minutes, longer than worst-case inference latency) makes it eligible for
  automatic retry, and after 3 failed attempts it is redriven to that
  stage's own DLQ for review.
- **Idempotent** — `doc_id` is the DynamoDB partition key, so a redelivered
  message safely overwrites the same item instead of duplicating it.
- **Immutable, shared image** — one Docker image runs both workers; the ECS
  `command` override selects `classification_worker.py` vs
  `extraction_worker.py` (see `lib/worker_stack.py`).
- **Least-privilege IAM** — classification-worker's task role can only
  consume its own queue; extraction-worker's task role can consume its own
  queue AND send (send-only, not consume) to classification-queue, since
  it owns the hand-off between stages. Each role writes *only* to
  `clinical-results` and calls `bedrock:InvokeModel` scoped to the specific
  pinned model ARN.
- **Queue-depth autoscaling** — each service scales independently, 1–10
  tasks, based on `ApproximateNumberOfMessagesVisible` on its own queue.

## Repository layout

```
workers/                 Shared worker image (Dockerfile + Python source)
  Dockerfile              Multi-stage build: venv builder -> slim, non-root runtime
  requirements.txt        boto3 / botocore
  classification_worker.py
  extraction_worker.py
  common/
    queue_client.py       SQS long-poll receive + delete-after-success
    llm_client.py         Isolated Amazon Bedrock (Nova) invocation
    db_client.py          Idempotent DynamoDB put_item
    audit.py              SHA-256 input hashing + structured audit logging
lib/                      AWS CDK (Python) stacks
  queue_stack.py           SQS queues + DLQs
  data_stack.py            DynamoDB `clinical-results` table
  worker_stack.py          VPC, ECS cluster, 2 Fargate services, IAM, autoscaling
bin/
  app.py                   CDK app entrypoint wiring all stacks together
scripts/
  seed_dummy_data.py       Pushes 10 synthetic documents to both queues
cdk.json, requirements.txt, package.json, tsconfig.json
```

> **Note on `package.json` / `tsconfig.json`:** this is a **Python** CDK
> app (`app.py`, per `cdk.json`). They're included here only for tooling
> parity with the requested layout: `package.json` provides convenience
> `npm run <script>` wrappers around the `cdk` CLI (which itself ships as
> an npm package, hence `npm install`), and `tsconfig.json` compiles
> nothing (no TypeScript source exists in this repo). All application and
> infrastructure logic is Python.

## Prerequisites

- Python 3.12+ and `pip`
- Node.js 20+ and `npm` (only to install the `cdk` CLI binary)
- Docker (for building the worker image during `cdk deploy`)
- An AWS account/credentials with permission to create the resources below,
  and **Bedrock model access enabled** for `amazon.nova-lite-v1:0` in your
  target region (enable this once in the Bedrock console under "Model
  access").
- AWS CDK v2 (installed via `npm install` below)

## Setup

```bash
# 1. Install the CDK CLI (via the provided package.json)
npm install

# 2. Create a Python virtualenv and install CDK app dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Bootstrap your AWS account/region for CDK (one-time per account/region)
npx cdk bootstrap
# or, using the npm script wrapper:
npm run bootstrap

# 4. Review and deploy all three stacks
npx cdk synth
npx cdk deploy --all
# or:
npm run deploy
```

`cdk deploy` will:
- Build the `workers/` Docker image locally and push it to an
  auto-created ECR asset repository.
- Create both SQS queues + DLQs, the DynamoDB table, the VPC (no NAT
  gateways — private isolated subnets + VPC endpoints only), the ECS
  cluster, and both Fargate services with autoscaling.

Take note of the `ClinicalPipelineQueueStack` outputs — you'll need the
`extraction-queue` URL for the seed script (it's the pipeline's sole entry
point; `classification-queue` is populated automatically by
extraction-worker). If outputs aren't printed by default, fetch them with:

```bash
aws cloudformation describe-stacks \
  --stack-name ClinicalPipelineQueueStack \
  --query "Stacks[0].Outputs"
```

(Add `CfnOutput`s in `lib/queue_stack.py` if you want them surfaced
automatically, or just read the queue URL directly from the SQS console --
it's named `extraction-queue`.)

## Seed synthetic demo data

```bash
source .venv/bin/activate
python scripts/seed_dummy_data.py \
  --extraction-queue-url https://sqs.<region>.amazonaws.com/<account>/extraction-queue
```

This sends 10 fabricated documents (fake names, `TEST-00xx` MRNs) to
**extraction-queue** only. Each document is processed by extraction-worker
first; only after that result is durably written and audited does
extraction-worker itself push the document onto `classification-queue` for
classification-worker to pick up. Both stages share the same `doc_id`, so
you can watch the document flow through the full pipeline sequentially.

## Viewing results

**DynamoDB** (`clinical-results` table, partition key `doc_id`):

```bash
aws dynamodb scan --table-name clinical-results --max-items 10
```

Each `doc_id` accumulates two writes over time, in order: extraction-worker
writes first (`entities` field), then classification-worker writes second
(`classification` field) once extraction has handed the document off. Both
writes are idempotent overwrites keyed on `doc_id`, so re-running the seed
script or a message redelivery never creates duplicates.

**CloudWatch Logs** (audit trail + application logs):

```bash
aws logs tail /ecs/classification-worker --follow
aws logs tail /ecs/extraction-worker --follow
```

Audit lines are JSON (`{"audit": true, "doc_id": ..., "worker": ...,
"model_id": ..., "input_hash": ..., "output_summary": ..., "timestamp":
...}`) and never contain raw extracted PHI — only a SHA-256 hash of the
input and a short, non-PHI output summary.

## Tearing down

```bash
npx cdk destroy --all
# or:
npm run destroy
```

Note: `clinical-results` DynamoDB table has `RemovalPolicy.RETAIN` (see
`lib/data_stack.py`) so it survives `cdk destroy` — delete it manually via
the console/CLI if you want a full cleanup:

```bash
aws dynamodb delete-table --table-name clinical-results
```

## How to run in 5 steps

1. `npm install && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. `npx cdk bootstrap`
3. `npx cdk deploy --all` (builds/pushes the worker image, creates all resources)
4. `python scripts/seed_dummy_data.py --extraction-queue-url <url>` (the script only
   accepts `--extraction-queue-url`; extraction-worker forwards documents to
   classification-queue automatically)
5. Watch results: `aws dynamodb scan --table-name clinical-results` and
   `aws logs tail /ecs/classification-worker --follow`
   / `aws logs tail /ecs/extraction-worker --follow`

When done: `npx cdk destroy --all`.

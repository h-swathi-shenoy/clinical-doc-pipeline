#!/usr/bin/env python3
"""CDK app entrypoint: wires up the queue, data, and worker stacks.

Stack wiring order matters: QueueStack and DataStack expose the queue and
table constructs that WorkerStack consumes directly (same CDK app / same
synthesis, so references are passed as live objects rather than through
SSM parameters or CloudFormation exports -- simplest and safest for a
single-account reference deployment).
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from lib.data_stack import DataStack
from lib.queue_stack import QueueStack
from lib.worker_stack import WorkerStack

app = cdk.App()

env = cdk.Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
)

queue_stack = QueueStack(app, "ClinicalPipelineQueueStack", env=env)
data_stack = DataStack(app, "ClinicalPipelineDataStack", env=env)

worker_stack = WorkerStack(
    app,
    "ClinicalPipelineWorkerStack",
    classification_queue=queue_stack.classification_queue,
    extraction_queue=queue_stack.extraction_queue,
    results_table=data_stack.results_table,
    image_asset_path="workers",
    env=env,
)
worker_stack.add_stack_dependency(queue_stack)
worker_stack.add_stack_dependency(data_stack)

app.synth()

# clinical-doc-pipeline
An **asynchronous, queue-driven** (no REST API) pipeline that processes synthetic medical documents using two **dependent, sequential** ECS Fargate worker services, each backed by its own SQS queue:

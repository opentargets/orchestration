# Containerization

Orchestration repository contains:

1. Docker image for building and running the Orchestration service.
2. Docker image for running the harmonisation jobs.

## Accessing images

Both images are available on Github Container Registry:

- `ghcr.io/open-targets/orchestration:latest`
- `ghcr.io/open-targets/orchestration:<version>`
- `ghcr.io/open-targets/orchestration-harmonisation:latest`
- `ghcr.io/open-targets/orchestration-harmonisation:<version>`

>[!NOTE] The images are built manually when a new version is released using the `artifact.yaml` workflow.

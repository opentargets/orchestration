#!/bin/bash
set -e

# tunnel to the machine
AIRFLOW_DEV_MACHINE_NAME=$(terraform -chdir=./deployment output -raw up_airflow_dev_vm)
PROJECT_ID="open-targets-eu-dev"
echo "Tunneling the remote airflow-apiserver to localhost:8081 from ${AIRFLOW_DEV_MACHINE_NAME}..."
gcloud -q compute ssh --zone="europe-west1-d" --project="${PROJECT_ID}" "$AIRFLOW_DEV_MACHINE_NAME" -- -fNL 8081:localhost:8080

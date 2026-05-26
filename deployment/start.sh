#!/bin/bash
set -e

cecho() {
  echo -e "\n\033[92m✨ ${1}\033[0m\n"
}

open_browser() {
  URL="$1"

  case "$(uname -s)" in
    Darwin)
      open "$URL"
      ;;
    Linux)
      xdg-open "$URL" >/dev/null 2>&1
      ;;
  esac
}

# set the env var for the orchestration git branch we want to use in the machine
export TF_VAR_orchestration_git_branch
TF_VAR_orchestration_git_branch=$(git branch --show-current)
PROJECT_ID="open-targets-eu-dev"

# check requirements
if ! command -v terraform &> /dev/null || ! command -v gcloud &> /dev/null || ! command -v code &> /dev/null;
then
    echo "This script requires terraform, gcloud, and visual studio code to be installed."
    exit 1
fi

# ensure the script is run from the root of the repository
if [ ! -d ".git" ]; then
    echo "This script must be run from the root of the repository."
    exit 1
fi

# check and deploy the airflow dev machine
cecho "Ensuring the airflow dev machine and Airflow stack are ready..."
terraform -chdir=./deployment init
set +e
terraform -chdir=./deployment plan -detailed-exitcode -out=plan.out
CHANGES_TO_BE_MADE="$?"
set -e
if [ $CHANGES_TO_BE_MADE -eq 2 ]; then
  terraform -chdir=./deployment apply -auto-approve plan.out
  cecho "Waiting for the airflow dev machine to boot..."
  sleep 25
fi

# wait for the machine to be ready
AIRFLOW_DEV_MACHINE_NAME=$(terraform -chdir=./deployment output -raw up_airflow_dev_vm)
while ! gcloud compute -q ssh --zone="europe-west1-d" --project="${PROJECT_ID}" "$AIRFLOW_DEV_MACHINE_NAME" --command="test -f /ready" > /dev/null 2>&1; do
  echo "  ...waiting 10 more seconds..."
  sleep 10
done

# install local dependencies in the machine's environment (used by vscode)
cecho "Installing local dependencies in the airflow dev machine"
gcloud -q compute ssh --zone="europe-west1-d" --project="${PROJECT_ID}" "$AIRFLOW_DEV_MACHINE_NAME" -- 'bash -s' < ./deployment/startup_user.sh

# tunnel to the machine
cecho "Tunneling the remote airflow-apiserver to localhost:8081 from ${AIRFLOW_DEV_MACHINE_NAME}..."
gcloud -q compute ssh --zone="europe-west1-d" --project="${PROJECT_ID}" "$AIRFLOW_DEV_MACHINE_NAME" -- -fNL 8081:localhost:8080

# start vs code remote
cecho "Starting vscode remote..."
gcloud -q compute config-ssh --project="${PROJECT_ID}" > /dev/null 2>&1
code --folder-uri "vscode-remote://ssh-remote+${AIRFLOW_DEV_MACHINE_NAME}.europe-west1-d.${PROJECT_ID}/opt/orchestration"

# open the browser to the airflow API/UI
cecho "Opening the Airflow API/UI..."
sleep 5
open_browser "http://localhost:8081"

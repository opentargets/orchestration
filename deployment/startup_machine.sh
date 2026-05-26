#!/bin/bash
set -x
set -e

# remove man
apt-get remove -y --purge man-db

# update package lists
apt-get update -y

# install dependencies
apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  git \
  build-essential

# add docker gpg key
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# add docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# update package lists again
apt-get update -y

# install docker
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# clone repository
mkdir -p /opt/orchestration
git clone https://github.com/opentargets/orchestration /opt/orchestration
cd /opt/orchestration
BRANCH=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/orchestration_git_branch)
git checkout "$BRANCH"

# set proper ownership and permissions
# all google cloud iam users are members of google-sudoers, so we use that group to avoid having to
# add groups to users which seems to be a mess in google cloud vms
chgrp -R google-sudoers /opt/orchestration
chmod -R g+rw /opt/orchestration

# create orchestration user
sudo useradd -m -G google-sudoers,docker orchestration

REMOTE_AIRFLOW_SERVICES="postgres airflow-init airflow-scheduler airflow-dag-processor airflow-triggerer airflow-apiserver"

fail_service_startup() {
  SERVICE_NAME="$1"
  cd /opt/orchestration
  docker compose ps --all "$SERVICE_NAME"
  docker compose logs --no-color --tail=50 "$SERVICE_NAME"
  exit 1
}

wait_for_airflow_init() {
  while true; do
    CONTAINER_ID=$(cd /opt/orchestration && docker compose ps --all -q airflow-init)
    if [ -n "$CONTAINER_ID" ]; then
      STATUS=$(docker inspect --format '{{.State.Status}}' "$CONTAINER_ID")
      EXIT_CODE=$(docker inspect --format '{{.State.ExitCode}}' "$CONTAINER_ID")
      if [ "$STATUS" = "exited" ] && [ "$EXIT_CODE" = "0" ]; then
        break
      elif [ "$STATUS" = "exited" ]; then
        fail_service_startup airflow-init
      fi
    fi
    sleep 5
  done
}

wait_for_healthy_service() {
  SERVICE_NAME="$1"
  while true; do
    CONTAINER_ID=$(cd /opt/orchestration && docker compose ps --all -q "$SERVICE_NAME")
    if [ -n "$CONTAINER_ID" ]; then
      STATUS=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$CONTAINER_ID")
      if [ "$STATUS" = "healthy" ]; then
        break
      elif [ "$STATUS" = "unhealthy" ] || [ "$STATUS" = "exited" ] || [ "$STATUS" = "dead" ]; then
        fail_service_startup "$SERVICE_NAME"
      fi
    fi
    sleep 5
  done
}

wait_for_apiserver() {
  until curl --fail --silent http://localhost:8080/api/v2/monitor/health > /dev/null; do
    sleep 5
  done
}

# run the Airflow stack used for remote development
su orchestration -c "
  cd /opt/orchestration &&
  AIRFLOW__API__SECRET_KEY=\$(openssl rand -hex 32) \
  AIRFLOW__API_AUTH__JWT_SECRET=\$(openssl rand -hex 32) \
  AIRFLOW__API_AUTH__JWT_ISSUER=airflow \
  docker compose up -d --build ${REMOTE_AIRFLOW_SERVICES}
"
wait_for_airflow_init
wait_for_healthy_service postgres
wait_for_healthy_service airflow-scheduler
wait_for_healthy_service airflow-dag-processor
wait_for_healthy_service airflow-triggerer
wait_for_healthy_service airflow-apiserver
wait_for_apiserver

# signal that the script is done
touch /ready

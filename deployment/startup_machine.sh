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
apt-get install -y docker-ce docker-ce-cli containerd.io

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

# run airflow
su orchestration -c "docker compose up -d"

# signal that the script is done
touch /ready

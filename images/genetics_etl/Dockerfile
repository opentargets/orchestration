FROM europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/gentropy:dev

ARG CLOUD_SDK_VERSION=452.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

RUN apt-get update && \
    apt-get install -y jq && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# required by google batch scripts
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
    --bash-completion=false \
    --path-update=false \
    --usage-reporting=false \
    --quiet \
    && rm -rf "${TMP_DIR}"

ENV PATH="$PATH:$GCLOUD_HOME/bin"

COPY tasks/ tasks/
ENTRYPOINT ["/bin/bash"]

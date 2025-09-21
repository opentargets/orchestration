FROM ubuntu:24.04
LABEL owner="opentargets"
LABEL description="Custom Airflow image with Google Cloud SDK"
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get -y install gcc mono-mcs && \
    apt-get -y install curl && \
    rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 1010 airflow
WORKDIR /home/airflow
USER airflow
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
RUN --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=README.md,target=README.md \
    uv sync --frozen --no-editable

USER 0
RUN curl -sSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz" | \
  tar -xzf - -C /tmp && \
  /tmp/google-cloud-sdk/install.sh --bash-completion=false --path-update=false --usage-reporting=false --quiet
ENTRYPOINT ["uv", "run"]

FROM google/cloud-sdk:489.0.0-stable AS gcloud-builder

FROM europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/gentropy:2.3.0-rc.9
RUN ln -s /app/.venv/bin/python3 /usr/bin/python3
COPY --from=gcloud-builder /usr/lib/google-cloud-sdk /opt/google-cloud-sdk
ENV PATH="/opt/google-cloud-sdk/bin:${PATH}"
COPY --chmod=0755 images/gentropy/scripts/harmonise-sumstats.sh harmonise-sumstats.sh

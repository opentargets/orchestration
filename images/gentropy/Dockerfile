FROM europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/gentropy:dev
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg\
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && apt-get update -y && apt-get install google-cloud-cli -y
COPY --chmod=0755 images/gentropy/scripts/harmonise-sumstats.sh harmonise-sumstats.sh

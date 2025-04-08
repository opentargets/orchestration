VERSION := $$(grep '^version' pyproject.toml | sed 's%version = "\(.*\)"%\1%')
.DEFAULT_GOAL := cloud-dev

### HOUSEKEEPING TARGETS ###
.PHONY: help version clean test check cloud-dev upload-ukb-ppp-bucket-readme upload-eqtl-catalogue-bucket-readme upload-finngen-bucket-readme upload-gwas-catalog-buckets-readme update-bucket-docs build-gentropy-gcs-image setup-harmonisation-test

help: ## Show the help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-36s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

version: ## Show the package version
	@echo $(VERSION)

clean: ## Clean the project
	@docker compose down
	@rm -rf logs dist .venv .pytest_cache .ruff_cache deployment/.terraform deployment/plan.out


### DEVELOPMENT TARGETS ###
test: ## Run unit tests
	@uv run pytest

check: format test ## run all checks

dev: .git/hooks/commit-msg  ## Prepare the local development environment
	@uv sync --all-extras --dev
	@uv run pre-commit install --hook-type commit-msg
	@GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json docker compose -f compose.yaml -f compose.local.yaml up -d

cloud-dev: ## Start the remote development environment and connect to it (default goal)
	@./deployment/start.sh


### OTHER TARGETS ###
upload-ukb-ppp-bucket-readme: ## ppload ukb_ppp_eur_data readme to the bucket
	@gsutil rsync docs/datasources/ukb_ppp_eur_data gs://ukb_ppp_eur_data/docs

upload-eqtl-catalogue-bucket-readme: ## upload eqtl_catalogue_data readme to the bucket
	@gsutil rsync docs/datasources/eqtl_catalogue_data gs://eqtl_catalogue_data/docs

upload-finngen-bucket-readme: ## upload finngen_data readme to the bucket
	@gsutil rsync docs/datasources/finngen_data gs://finngen_data/docs

upload-gwas-catalog-buckets-readme: ## upload gwas_catalog readme to the bucket(s)
	@gsutil rsync docs/datasources/gwas_catalog_data gs://gwas_catalog_inputs/docs
	@gsutil rsync docs/datasources/gwas_catalog_data gs://gwas_catalog_sumstats_pics/docs
	@gsutil rsync docs/datasources/gwas_catalog_data gs://gwas_catalog_sumstats_susie/docs
	@gsutil rsync docs/datasources/gwas_catalog_data gs://gwas_catalog_top_hits/docs

update-bucket-docs: upload-eqtl-catalogue-bucket-readme upload-ukb-ppp-bucket-readme upload-finngen-bucket-readme upload-gwas-catalog-buckets-readme ## upload readmes to the datasource buckets

build-gentropy-gcs-image: ## build image that overwrited gentropy with tools specific for orchestration and google cloud
	@docker buildx build \
		--platform=linux/amd64,linux/arm64 \
		-t europe-west1-docker.pkg.dev/open-targets-genetics-dev/gentropy-app/ot_gentropy:dev  \
		--push \
		-f images/gentropy/Dockerfile \
		--no-cache .

setup-harmonisation-test: ## prepare the test bucket with raw summary statistics for the harmonisation test.
	@gsutil rm gs://ot_orchestration/test/gwas_catalog_inputs/harmonisation_manifest.csv
	@gsutil -m rm -r gs://ot_orchestration/test/gwas_catalog_inputs/harmonisation_summary
	@gsutil -m rm -r gs://ot_orchestration/test/gwas_catalog_inputs/harmonised_summary_statistics
	@gsutil -m rm -r gs://ot_orchestration/test/gwas_catalog_inputs/summary_statistics_qc

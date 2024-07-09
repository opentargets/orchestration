# How to set up the repository

> [!NOTE]
> The code in this repository is compatible with Linux and Mac only.

## 1. System requirements

Make sure you have [Docker](https://docs.docker.com/get-docker/) and [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed on your system.

> [!WARN]
> On macOS, the default amount of memory available for Docker might not be enough to get Airflow up and running. Allocate at least 4GB of memory for the Docker Engine (ideally 8GB). [More info](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#)

## 2. Development environment

Run `make dev`. This command will install Pyenv, Python 3.10, Poetry, pre-commit, initialise the virtual environment, and build the Airflow image.

## 3. Set up Google Cloud access

> [!WARN]
> Run the next two command with the appropriate Google Cloud project ID and service account name to ensure the correct Google default application credentials are set up.

Authenticate to Google Cloud:

```bash
gcloud auth application-default login --project=<PROJECT>
```

Create the service account key file that will be used by Airflow to access Google Cloud Platform resources:

```bash
gcloud iam service-accounts keys create ~/.config/gcloud/service_account_credentials.json --iam-account=<PROJECT>@appspot.gserviceaccount.com
```

## 4. Set Airflow user ID

> [!NOTE]
> These commands allow Airflow running inside Docker to access the credentials file which was generated earlier.

```bash
# If any user ID is already specified in .env, remove it.
grep -v "AIRFLOW_UID" .env > .env.tmp
# Add the correct user ID.
echo "AIRFLOW_UID=$(id -u)" >> .env.tmp
# Move the file.
mv .env.tmp .env
```

## 5. Start Airflow

Run `docker compose up` and open localhost:8080 on your local machine. Default username and password are both `airflow`.

# Additional information

## Managing Airflow and DAGs

The orchestration uses the Airflow container that is set up by the `docker compose`. To build the image standalone, run `make build-airflow-image`.

The airflow DAGs sit in the `orchestration` package inside the `dags` directory. The configuration for the DAGs is located in the `orchestration` package inside the `configs` directory.

Currently the DAGs are under heavy development, so there can be issues while Airflow tries to parse them. Current development focuses on unification of the `gwas_catalog_*` dags in `gwas_catalog_dag.py` file in a single DAG. To be able to run it one need to provide the configuration from the `configs/config.json` to the dag trigger as in the exaple picture.

![alt text](docs/image.png)

## Cleaning up

At any time, you can check the status of your containers with:

```bash
docker ps
```

To stop Airflow, run:

```bash
docker compose down
```

To cleanup the Airflow database, run:

```bash
docker compose down --volumes --remove-orphans
```

## Advanced configuration

More information on running Airflow with Docker Compose can be found in the [official docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1. **Increase Airflow concurrency**. Modify the `docker-compose.yaml` and add the following to the x-airflow-common → environment section:

   ```yaml
   AIRFLOW__CORE__PARALLELISM: 32
   AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 32
   AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY: 16
   AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
   # Also add the following line if you are using CeleryExecutor (by default, LocalExecutor is used).
   AIRFLOW__CELERY__WORKER_CONCURRENCY: 32
   ```

1. **Additional pip packages**. They can be added to the `requirements.txt` file.

## Troubleshooting

Note that when you a a new workflow under `dags/`, Airflow will not pick that up immediately. By default the filesystem is only scanned for new DAGs every 300s. However, once the DAG is added, updates are applied nearly instantaneously.

Also, if you edit the DAG while an instance of it is running, it might cause problems with the run, as Airflow will try to update the tasks and their properties in DAG according to the file changes.

# Google Batch job setup

This document explains how to configure and wire a Google Batch job into an Airflow DAG using the orchestration framework's batch operator stack.

## Architecture overview

The batch system is built around two Airflow operators that work together:

```
BatchIndexOperator  ──(list[BatchIndexRow])──►  BatchJobOperator (expanded)
   (pure-Python task)                              (one GCP Batch job per row)
```

1. **`BatchIndexOperator`** — runs in the Airflow worker. It calls a _manifest generator_ to inspect GCS data and produce a list of `BatchIndexRow` objects, each carrying an `EnvironmentRegistrySpec` (one `EnvironmentSpec` per Batch task). Rows are produced by partitioning the full task list using `max_task_count`.
2. **`BatchJobOperator`** — wraps `CloudBatchSubmitJobOperator`. One instance is expanded per `BatchIndexRow` via Airflow's [dynamic task mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html). Each invocation submits one GCP Batch job with its slice of per-task environments injected at runtime.

### Model hierarchy

```
BatchJobOperatorSpec
└── JobSpec
    ├── TaskGroupSpec
    │   ├── parallelism
    │   ├── TaskConfiguration
    │   │   ├── InstanceResourceSpec   (cpu_milli, memory_mib, boot_disk_mib)
    │   │   ├── RunnableSpec           (image_uri, script_file | inline_commands)
    │   │   ├── max_retry_count
    │   │   └── max_run_duration
    │   └── EnvironmentRegistrySpec    (populated at runtime by BatchIndexOperator)
    ├── AllocationSpec
    │   ├── InstanceSpec               (machine_type, provisioning_model)
    │   └── region | zones
    └── LogsSpec
```

Each model exposes a `.build()` method that returns the corresponding `google.cloud.batch_v1` protobuf object.

---

## Step 1 — Choose or write a manifest generator

A manifest generator implements `ProtoManifestGenerator` (see [src/orchestration/operators/batch/manifest_generators/proto.py](../../src/orchestration/operators/batch/manifest_generators/proto.py)):

```python
class ProtoManifestGenerator(Protocol):
    @classmethod
    def from_generator_config(cls, specs: ManifestGeneratorSpec) -> ProtoManifestGenerator: ...

    def generate_batch_index(self) -> BatchIndex: ...
```

The generator is responsible for:

1. Reading from GCS (or any source) to enumerate the tasks.
2. Returning a `BatchIndex` whose `EnvironmentRegistrySpec` holds one `EnvironmentSpec` per task. The variables in that spec are exposed as environment variables inside each Batch container.

Built-in generators and their registry keys:

| Registry key | Generator class | Used by |
|---|---|---|
| `finemapping` | `FinemappingManifestGenerator` | SuSiE finemapping DAG |
| `gentropy_step` | `GentropyStepManifestGenerator` | Unified pipeline |
| `harmonisation` | `HarmonisationManifestGenerator` | GWAS harmonisation DAG |
| `vep` | `VepManifestGenerator` | VEP annotation DAG |

To add a new generator:

1. Create `src/orchestration/operators/batch/manifest_generators/<name>.py` implementing `ProtoManifestGenerator`.
2. Register it in `MANIFEST_GENERATOR_MAP` in [batch_index_operator.py](../../src/orchestration/operators/batch/batch_index_operator.py).

---

## Step 2 — Write the DAG YAML config

All batch configuration lives in `src/orchestration/dags/config/<dag_name>.yaml`. A minimal config for a new batch DAG looks like:

```yaml
nodes:
  - id: generate_my_index
    kind: Task
    prerequisites: []
    google_batch_index_specs:
      pointer: finemapping           # key in MANIFEST_GENERATOR_MAP
      max_task_count: 40000          # tasks per GCP Batch job; 0 = all in one job
      generator_specs:
        generator_options:
          collected_loci_path: 'gs://my-bucket/input'
          output_path: 'gs://my-bucket/output'
          # ...any options required by the chosen generator

  - id: my_batch_job
    kind: Task
    prerequisites:
      - generate_my_index
    google_batch:
      job:
        task_group:
          parallelism: 40000         # max concurrent tasks within one Batch job
          task_environments:
            environments: []         # leave empty — populated at runtime
          task_config:
            max_retry_count: 1
            max_run_duration: "7200s"
            instance_resource_spec:
              cpu_milli: 4000        # 4 vCPUs
              memory_mib: 25000      # ~24 GiB
              boot_disk_mib: 20000   # 20 GiB
            runnable_spec:
              image_uri: 'ghcr.io/opentargets/gentropy:3.2.0'
              entrypoint: /usr/bin/bash
              script_file: my_script.sh   # relative to src/orchestration/assets/
              # OR use inline_commands: ["python", "-m", "my_module"]
        allocation:
          instance:
            machine_type: n2-highmem-4
            provisioning_model: SPOT     # SPOT | STANDARD | PREEMPTIBLE
        logs: {}
```

### Key config fields

| Field | Type | Description |
|---|---|---|
| `pointer` | `str` | Registry key for the manifest generator |
| `max_task_count` | `int` | Maximum tasks per Batch job. Total tasks are split across jobs of this size. `0` means no split. |
| `parallelism` | `int` | Maximum concurrently running tasks within a single Batch job |
| `cpu_milli` | `int` | CPU in millicores (1000 = 1 vCPU) |
| `memory_mib` | `int` | RAM in mebibytes (1024 MiB ≈ 1 GiB) |
| `boot_disk_mib` | `int` | Boot disk in mebibytes |
| `machine_type` | `str` | Must match `n\d-(standard\|highmem\|highcpu)-\d+` |
| `provisioning_model` | `str` | `SPOT` is cheapest; `STANDARD` for tasks that must not be preempted |
| `script_file` | `str` | Shell script name in `src/orchestration/assets/`. Mutually exclusive with `inline_commands` |

### Script files

If you use `script_file`, place the shell script under `src/orchestration/assets/`. The framework strips comments and blank lines, joins continuation lines (`\`), splits on `;`, and joins everything with `&&` before passing the result to the container entrypoint. Use `script_variables` to inject values:

```yaml
runnable_spec:
  script_file: my_script.sh
  script_variables:
    input_path: 'gs://bucket/input'
    output_path: 'gs://bucket/output'
```

In `my_script.sh` reference them as `${input_path}` and `${output_path}`.

---

## Step 3 — Wire up the DAG

```python
from orchestration.models.batch import BatchIndexOperatorSpec, BatchJobOperatorSpec
from orchestration.operators.batch import BatchIndexOperator, BatchJobOperator

index_config = find_node_in_config(config["nodes"], "generate_my_index")
job_config   = find_node_in_config(config["nodes"], "my_batch_job")

batch_index = BatchIndexOperator(
    task_id=index_config["id"],
    batch_index_specs=BatchIndexOperatorSpec(**index_config["google_batch_index_specs"]),
)

batch_job = BatchJobOperator.partial(
    task_id=job_config["id"],
    job_name="my-batch-job",
    batch_job_spec=BatchJobOperatorSpec(**job_config["google_batch"]),
).expand(batch_index_row=batch_index.output)

chain(batch_index, batch_job)
```

See [gwas_catalog_sumstats_susie_finemapping.py](../../src/orchestration/dags/gwas_catalog_sumstats_susie_finemapping.py) for a full working example.

---

## Partitioning and task counts

`BatchIndexOperator` produces one `BatchIndexRow` per Batch job. The number of rows is determined by:

```
n_jobs = ceil(total_tasks / max_task_count)
```

Each row carries the slice of `EnvironmentRegistrySpec` for that job. `BatchJobOperator` receives the rows via `.expand()` and submits one independent GCP Batch job per row, with the environments injected into `TaskGroup.task_environments`.

GCP Batch limits the number of tasks per job. If in doubt, use `max_task_count: 10000` as a conservative ceiling.

---

## Retry behaviour

Tasks are retried on the following GCP-reserved exit codes by default:

| Exit code | Meaning |
|---|---|
| 50001 | Agent reboot |
| 50002 | Agent restart |
| 50003 | Agent restart due to memory pressure |
| 50004 | Agent restart due to loss of connectivity |
| 50005 | Agent shutdown |

Set `max_retry_count: 0` (the default) to disable retries, or increase it for tasks susceptible to SPOT preemption. Provide `exit_codes` in `TaskConfiguration` to override the default set.

---

## Location constraints

`AllocationSpec` accepts either a `region` or a list of `zones`, but not both:

```yaml
allocation:
  instance:
    machine_type: n1-standard-4
    provisioning_model: SPOT
  # Default: region is set to the value of GCP_REGION env var (europe-west1)
  # Override with a specific region:
  region: regions/us-central1
  # OR constrain to specific zones:
  zones:
    - zones/europe-west1-b
    - zones/europe-west1-c
```

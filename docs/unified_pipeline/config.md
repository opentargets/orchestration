# Unified Pipeline pipeline config

This document describes the **Unified Pipeline configuration**

## Run version and output location

Every pipeline run is identified by a `run_name` set in `src/orchestration/dags/config/unified_pipeline.yaml`. This value controls where all pipeline outputs are written in Google Cloud Storage.

### `run_name` format

```
<prefix>/<flavor>-YYMM-N
```

| Part | Description | Example |
|------|-------------|---------|
| `prefix` | Your personal/team identifier — lowercase letter start, then letters or digits | `sz`, `pt01` |
| `flavor` | `platform` for the standard run path, `ppp` for the Partner Preview path; also determines downstream labeling | `platform` |
| `YYMM` | Two-digit year + two-digit month. Format-only validation (any four digits accepted) | `2605` |
| `N` | Revision number, starting from 1. Increment if re-running the same run definition. | `1` |
| `is_ppp` | Auto-derived from flavor — `true` when flavor is `ppp`, `false` otherwise | derived |

Valid examples: `sz/platform-2605-1`, `abc/ppp-2606-2`

### Output location and promotion

Every unified pipeline run writes to `gs://open-targets-pipeline-runs/<run_name>`.

The DAG does not perform a separate production-mode execution. If outputs need to be published to a release location, that promotion happens after the run and outside this configuration.

### PPP mode (derived from run_name)

PPP configuration overrides are auto-enabled whenever the `flavor` portion of `run_name` is set to `ppp`:

- `run_name: 'sz/ppp-2605-1'` → PPP mode enabled (`is_ppp = True`)
- All steps tagged with `ppp_only: true` are included in the DAG.
- Override configs from `src/orchestration/dags/config/ppp/` are loaded.

For the standard unified-pipeline path, use `platform` as the flavor:

- `run_name: 'sz/platform-2605-1'` → PPP mode excluded (`is_ppp = False`)

### `release_name`

`release_name` remains the canonical downstream label derived from `run_name` as `<flavor>-<YYMM>`.

Examples:

- `sz/platform-2605-1` → `platform-2605`
- `sz/ppp-2605-1` → `ppp-2605`

PTS and Gentropy consume `release_name`, while the full `run_name` continues to identify the concrete pipeline run and its output path.

## Unified Pipeline configuration

The UP (unified pipeline) configuration is defined in the `src/orchestration/dags/config` directory. The configuration is split into 4 main components, that when rendered together, form the complete configuration for the unified pipeline:

- `pis.yaml` - configuration for the Platform Input Stage (PIS)
- `pts.yaml` - configuration for the Platform Transformation Stage (PTS)
- `etl.conf` - configuration for the Platform ETL backend (ETL)
- `gentropy.yaml` - configuration for the Platform Genetics (Gentropy)

Typically the configuration file has to have at least the `steps` key defined that marks all of the pipeline steps that should be executed within the pipeline run.

```
steps:
    steps:
        biosample:
            - name: copy cell ontology
            source: cl.json
            destination: input/biosample/cl.json
```

Each step config (_biosample_ in the example above) should hold the definition of the step parameters required to execute the step.

Each config structure is unique to the tool. Refer to the specific tool documentation for more details on the configuration structure.

> WARNING!
> For ETL the configuration is provided in the `hocon` format, due to historical reasons.

### Template variables

The template variables can be used in the configuration files to define the dynamic parts of the configuration. Currently one has to register the template variables in the `src/orchestration/dags/config/unified_pipeline.py` file, which is then used to render the configuration files.

### Infrastructure configuration

Along with that the configuration also includes infrastructure specific config, which defines the Spark clusters in `clusters.yaml`.

## Overriding configurations

> NOTE!
> The functionality described here can be used in the PPP (Partner Platform Preview) unified pipeline runs, as it allows to override specific parts of the configuration.

In order to override the default configuration one can define the configuration files in the `src/orchestration/dags/config` directory. By convention, the override files should be named as `${pipeline_part}.override.yaml` where `${pipeline_part}` is of:

- pis
- pts
- etl
- gentropy

To override the specific configuration one need to define the same config file as the original one defined in the `src/orchestration/dags/config` directory, but with the `.override.yaml` suffix. For example, to override the `pis.yaml` configuration, one should create a file named `src/orchestration/dags/config/ppp/pis.override.yaml`.

> IMPORTANT!
> The override functionality is performed on the **rendered and parsed** configs!
> Any template variables in the override files will be parsed with the templates from the original config files.
> Only the content of the `steps` key will be overridden, the rest of the configuration will be dropped after rendering.

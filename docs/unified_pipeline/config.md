# Unified Pipeline pipeline config

This document describes the **Unified Pipeline configuration**

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

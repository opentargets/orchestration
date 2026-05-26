# Orchestration documentation

This catalog describes how the orchestration works in the current state

## How to generate dag svg files for documentation

```{bash}
make build-dags-svg
```

## How to update dag documentation on staging buckets

```{bash}
make update-bucket-docs
```

This documentation is updated manually on each data or staging data update.

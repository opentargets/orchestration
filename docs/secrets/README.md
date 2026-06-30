# Secret injection

Secrets are fetched from GCP Secret Manager at runtime and never embedded in code or config. Two compute backends are supported, each with a different injection mechanism.

| Backend | How | Where secrets land |
| --- | --- | --- |
| Dataproc | Generated bash init action script, uploaded to GCS | `/var/run/secrets/{secret_id}` on every cluster node |
| Google Batch | Native `secret_variables` in `batch_v1.Environment` | Environment variables inside each task container |

---

## Dataproc

At cluster creation time, `CreateClusterOperator` generates a bash script, uploads it to `secret_init_action_uri`, and registers it as a `NodeInitializationAction`. The script runs on every node before tasks start and writes files under `/var/run/secrets/` (owner `root:hadoop`, mode `440`).

There are two formats depending on how the secret is stored in Secret Manager.

### `secret_map` — plain string secrets

Use when the secret value is a plain string (token, password). The script wraps it in a `{"ENV_VAR": "value"}` envelope:

```
/var/run/secrets/hfhub-key  →  {"HF_TOKEN": "hf-abc123..."}
```

```yaml
dataproc:
  cluster_config:
    secret_map:
      HF_TOKEN: hfhub-key
    secret_init_action_uri: 'gs://opentargets-pipelines/up/gentropy/fetch_secrets.sh'
```

### `secret_blob_list` — JSON blob secrets

Use when the secret value is itself a JSON object (credentials file, config blob). The script writes it **verbatim** — no envelope wrapping, which would corrupt the JSON:

```
/var/run/secrets/decode  →  {"client_id": "...", "client_secret": "..."}
```

```yaml
dataproc:
  cluster_config:
    secret_blob_list:
      - decode
    secret_init_action_uri: 'gs://opentargets-pipelines/up/gentropy/fetch_secrets.sh'
```

Both can be combined in the same cluster config. `secret_init_action_uri` is required whenever either field is set.

---

## Google Batch

Google Batch natively supports secret injection via `batch_v1.Environment.secret_variables`. The framework maps the `Secrets` model into that field — GCP resolves the values at task startup and exposes them as environment variables inside the container. No init script is needed.

```yaml
google_batch:
  job:
    task_group:
      task_config:
        shared_environment:
          secrets:
            mapping:
              HF_TOKEN:
                secret_id: hfhub-key
```

Only plain-string secrets (`Secrets`/`secret_map`) are supported here. `SecretBlobs` is Dataproc-only — the Batch API does not write raw files to disk.

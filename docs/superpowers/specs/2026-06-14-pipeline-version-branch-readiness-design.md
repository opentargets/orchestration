# Pipeline Version Branch Readiness Design

## Goal

Finish the in-progress unified-pipeline migration so the `pipeline-version` branch is internally consistent, documented correctly, and ready for a merge-readiness verification pass.

## Scope

This design covers the already-started migration away from legacy `is_dev` handling in unified-pipeline configuration.

It includes:
- finalizing the `run_name`-driven model in code
- keeping `release_name` as a derived downstream label only
- rejecting legacy `is_dev` configuration early and explicitly
- aligning unified-pipeline docs, config comments, and tests with the new behavior
- running a bounded consistency sweep for nearby stale references on the branch

It does not include:
- changing downstream product semantics beyond the current migration
- reintroducing production-bucket behavior or backward compatibility for `is_dev`
- unrelated refactors outside the unified-pipeline run-configuration area

## Recommended Approach

Use a single consistency pass to complete the existing migration, then do a bounded branch-readiness sweep around the same feature area.

This is the recommended middle path because it finishes the already-modified files as one coherent change while still catching nearby contradictions that would make the branch unsafe to merge.

## Alternatives Considered

### 1. Minimal finish only

Only complete the currently edited files and stop.

Pros:
- smallest change set
- fastest path to code completion

Cons:
- higher risk of leaving stale `is_dev` or release-bucket references nearby
- more likely to fail review or require follow-up fixups before merge

### 2. Broad branch cleanup

Search and normalize every related reference across the branch, even if farther away from the touched files.

Pros:
- strongest consistency guarantees

Cons:
- increased scope
- higher risk of pulling in unrelated churn

## Architecture

### `PipelineRunConfig`

`PipelineRunConfig` is the single authority for unified-pipeline run identity.

Responsibilities:
- validate `run_name` format
- reject zero or negative-style revisions such as `...-0`
- derive `is_ppp` from the `run_name` flavor
- derive `release_name` as `<flavor>-<YYMM>` for downstream consumers
- emit `release_uri` only under `gs://open-targets-pipeline-runs/<run_name>`

Non-responsibilities:
- no `is_dev` input
- no production-bucket branching
- no promotion logic

### `UnifiedPipelineConfig`

`UnifiedPipelineConfig` remains responsible for loading `unified_pipeline.yaml`, constructing the validated run object, and wiring derived values into PIS, PTS, ETL, and Gentropy configs.

New invariant:
- if `is_dev` is present in `unified_pipeline.yaml`, config loading fails immediately before any downstream stage config is consumed

This keeps legacy behavior from being silently accepted and prevents partial configuration from leaking deeper into DAG setup.

### Documentation and Config Comments

The unified-pipeline docs and inline YAML comments must all describe the same execution model:
- `run_name` identifies the concrete run
- every run writes to `gs://open-targets-pipeline-runs/<run_name>`
- PPP behavior is derived from `run_name`
- later promotion to a release location happens outside the DAG
- `release_name` is still derived for downstream labeling, not storage selection

### Tests

Regression tests should cover:
- accepted and rejected `run_name` values
- explicit rejection of legacy `is_dev` model input
- `release_uri` always using the pipeline-runs bucket
- `release_name` derivation
- fail-fast rejection of `is_dev` in `UnifiedPipelineConfig` before stage config loading

## Data Flow

1. `UnifiedPipelineConfig` reads `unified_pipeline.yaml`.
2. If `is_dev` exists in the root config, initialization raises immediately with a migration-oriented error.
3. `run_name` is validated by `PipelineRunConfig`.
4. `PipelineRunConfig` derives `is_ppp`, `release_name`, and `release_uri`.
5. Stage configs render from those derived values.
6. PPP-only steps and overrides are enabled only when `run_name` flavor is `ppp`.

## Error Handling

- malformed `run_name` raises validation errors with the expected format
- zero revision raises a validation error that explains revisions start at 1
- legacy `is_dev` in YAML raises a direct configuration error before any stage config loads
- no compatibility fallback is provided for old production-bucket semantics

## Testing and Verification

Primary verification:
- run focused tests for `tests/test_run_config.py`
- run focused tests for `tests/test_unified_pipeline_config.py`

Branch-readiness sweep:
- search the unified-pipeline area for lingering `is_dev` references
- search for stale references to production-bucket output semantics that contradict the new model
- inspect docs/config examples for wording drift around `run_name`, PPP mode, and promotion

Known constraint at design time:
- the current worktree environment does not have `pytest` available on `PATH`, so test execution may require the project’s normal environment/bootstrap command before verification can complete

## Files In Scope

- `src/orchestration/models/run_config.py`
- `src/orchestration/dags/config/unified_pipeline.py`
- `src/orchestration/dags/config/unified_pipeline.yaml`
- `tests/test_run_config.py`
- `tests/test_unified_pipeline_config.py`
- `docs/unified_pipeline/README.md`
- `docs/unified_pipeline/config.md`

Additional files may be touched only if the bounded consistency sweep finds directly conflicting references in the same feature area.

## Merge-Readiness Exit Criteria

- code, tests, docs, and config comments all describe the same post-`is_dev` behavior
- legacy `is_dev` input is rejected in both model-level and config-loading paths where applicable
- no known stale references remain in the bounded unified-pipeline sweep
- targeted verification has been run in the project’s supported environment

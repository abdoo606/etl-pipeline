# 17 · etl

An idempotent, checkpointed ETL pipeline with retries.

## Run

```bash
from etl.pipeline import Pipeline, Checkpoint
```

## Test

```bash
pytest projects/17-etl-pipeline -q
```

## What it demonstrates

- Checkpointed cursor: a crash resumes instead of reprocessing
- Idempotent by construction - a second run of the same batch loads nothing
- Retries with exponential backoff around both source and sink
- Rejected records are counted and sampled, never silently dropped

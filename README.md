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

## Repository
- GitHub: https://github.com/{owner}/etl-pipeline
- Python 3.11+ · tests: `pytest` · runtime dependencies: none (standard library only)

# StepRight Lakehouse

A medallion-architecture data pipeline (Bronze → Silver → Gold) built on Databricks, with automated CI (lint + unit tests) and CD (Git-sourced job deployment) via GitHub Actions and GitHub integration.

## Architecture

```
Bronze (raw ingestion)
   │  file-based sources + CDC sources, per-source data quality checks
   ▼
Silver (cleaned, conformed)
   │  SCD1 and SCD2 history tracking, append-only streams
   ▼
Gold (business marts)
   │  customer_360, daily_revenue, fulfillment_health,
   │  funnel_analysis, product_performance
```

Each layer runs as an independent Databricks Job, deployed via Git integration — jobs pull notebooks directly from this repo's `main` branch rather than a static workspace copy, so a merge to `main` is what ships to production.

### Bronze
- Ingests both file-based sources (categories, clickstream, inventory, products) and CDC sources (customers, orders, order_items).
- Each source has a paired data-quality (`_dq`) check task.
- A `bronze_complete_flag` task marks successful completion of the full layer.

### Silver
- Applies SCD1 (overwrite-on-change) and SCD2 (full history) logic depending on the source.
- An append-only path handles sources that don't need change tracking.
- A `silver_complete` flag confirms the layer finished.

### Gold
- Business-facing marts, each split into two files per mart:
  - `<mart>_logic.py` — a **pure transformation function** (no I/O, no `dbutils`, no `current_date()`). Takes DataFrames and a fixed `as_of_date` in, returns a DataFrame out.
  - `<mart>.py` — the orchestration wrapper: reads real tables, calls the logic function, writes/merges the result.
- This split is what makes the logic functions unit-testable without a live cluster or real tables — see **Testing** below.
- Current marts: `gold_customer_360`, `gold_daily_revenue`, `gold_fulfillment_health`, `gold_funnel_analysis`, `gold_product_performance`.
- A `gold_complete_flag` task confirms the layer finished.

## Repo structure

```
bronze/
  01_bronze_ingestion/
    cdc/                  — CDC source ingestion notebooks
    files/                — file-based source ingestion notebooks
  02_bronze_dq/            — data quality check notebooks
  bronze_complete_flag.ipynb

silver/
  scd1/                    — SCD1 notebooks
  scd2/                    — SCD2 notebooks
  append/                  — append-only notebooks
  silver_complete_flag.ipynb

gold/
  gold_customer_360.py / gold_customer_360_logic.py
  gold_daily_revenue.py / gold_daily_revenue_logic.py
  gold_fulfillment_health.py / gold_fulfillment_health_logic.py
  gold_funnel_analysis.py / gold_funnel_analysis_logic.py
  gold_product_performance.py / gold_product_performance_logic.py
  gold_complete_flag.py

tests/
  conftest.py              — shared pytest fixtures (local PySpark session, import path setup)
  gold/
    test_gold_customer_360_logic.py

.github/workflows/
  ci.yml                   — GitHub Actions: lint + unit tests on every push/PR to main

ruff.toml                  — lint configuration (Databricks runtime globals, relaxed style rules)
```

## CI — Continuous Integration

Every push or pull request to `main` triggers `.github/workflows/ci.yml`, which runs two jobs:

**`lint`** — checks every `.py` file (with `ruff`) and every `.ipynb` notebook (with `nbqa`, which lints the code cells inside notebooks) across `bronze/`, `silver/`, and `gold/`.

**`test`** — installs PySpark + Java, then runs `pytest tests/` against the pure logic functions in Gold. Tests fabricate small DataFrames and a fixed `as_of_date`, so they run without any real cluster, table, or dependency on wall-clock time.

`ruff.toml` at the repo root tells the linter about Databricks' runtime-injected globals (`spark`, `dbutils`, `display`, etc.) so they aren't flagged as undefined, and relaxes a few style-only rules (import ordering, unused unpacked variables) that were deferred rather than fixed.

## CD — Continuous Deployment

Each Databricks Job (Bronze, Silver, Gold) is configured with **Source: Git**, pointed at this repo's `main` branch. Task paths are relative to the repo root (e.g. `bronze/01_bronze_ingestion/files/files_bronze_ingestion`), matching the folder structure above. This means:

- Merging to `main` is what actually changes what runs in production — no manual copy-paste from a dev notebook into a "real" workspace copy.
- Bronze is fully converted and verified end-to-end. Silver and Gold are being converted the same way.

## Running tests locally

```bash
pip install pytest pyspark
pytest tests/ -v
```

`tests/conftest.py` adds the repo root to `sys.path` so tests can import Gold logic files directly (e.g. `from gold.gold_customer_360_logic import compute_customer_360`), and provides a session-scoped local PySpark session shared across all tests.

## Known limitations

- **No cross-job orchestrator.** Databricks Free Edition's "Run Job" task type does not reliably surface other jobs in its picker, so Bronze → Silver → Gold are not currently chained into a single orchestrator job. Each layer is run independently (or could be chained via the Databricks REST API as a future enhancement). This is a Free Edition constraint, not a design choice — this same orchestration pattern has been implemented elsewhere without issue on non-Free tiers.
- A handful of style-only lint rules are relaxed rather than fixed (see `ruff.toml`) — these are cosmetic (import ordering, an intentionally-unused unpacked variable, timezone-naive `date.today()` in one Gold mart) rather than correctness issues, and can be revisited by removing them from the `ignore` list.

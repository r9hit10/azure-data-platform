# Azure Data Platform — 3 projects on one reusable ADF + Databricks framework

| # | Project | Status |
|---|---|---|
| 1 | **Banking** — 3 heterogeneous sources (REST API, Azure SQL, on-prem SQL Server via Self-hosted IR) → metadata-driven watermark ingestion → Databricks Silver/Gold | ✅ ready — `docs/P1_BANKING_BUILD_STEPS.md` |
| 2 | **E-commerce** — JSON CDC watermarks, back-dated refresh, REST API pagination, SCD1/SCD2, late-arriving dimensions | code in `adf/`, `databricks/ecom`, `sql/ecom` (guide coming) |
| 3 | **Supply chain** — event triggers, Auto Loader, Spark performance tuning | coming |

```
sql/         00_control_framework (config, watermark, audit) · bank/ · onprem/ · ecom/
api/         crm_customers_api — Azure Function serving customers with pagination + modified_since
adf/         Data Factory resources (Git-integration format) — point ADF Git "Root folder" here
databricks/  common/ (config, utils: MERGE, SCD2, DQ, recon, Auto Loader) · bank/ · ecom/
config/      ecom_tables.json (upload to opsfinserve/config/ for Project 2)
docs/        step-by-step build guides
```

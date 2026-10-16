# Project 1 — Banking: build steps (3 sources → ADF → Databricks)

## Your resource names
| What | Name |
|---|---|
| Resource group | `rg-finserve-dev` |
| Storage (ADLS Gen2) | `stfinservedevrohit` — containers `landingfinserve`, `bronzefinserve`, `silverfinserve`, `goldfinserve`, `opsfinserve` |
| Azure SQL | server `sql-finserve-rohit`, database `sqldb-finserve-rohit` |
| Data Factory | `adf-finserve-rohit` |
| Self-hosted IR | `ir-selfhosted-laptop` (node ACER-PREDAT0R) ✅ |
| Databricks | `dbw-finserve-dev`, access connector `ac-finserve-dev` |

## Architecture
```
 Azure SQL ─ customers ─────────────────────────┐   source_type = AZURE_SQL
 Azure SQL (sqldb-finserve-rohit) ─ accounts ──┼─► ADF pl_ingest_master ─ Lookup ctl.ingestion_config
 Laptop SQL Server (via SHIR) ─ transactions ──┘        └─ ForEach ─ Switch(source_type) ─► 3 child pipelines
                                                             (watermark incremental, audit log, Fail on error)
        ▼
 bronzefinserve/bank/<table>/load_date=YYYY-MM-DD/*.parquet
        ▼  Databricks: Auto Loader + DQ + dedupe + Delta MERGE + recon
 silverfinserve/bank/<table>   (+ _quarantine)          → dataplatform.bank_silver.*
        ▼
 goldfinserve/bank/<table>     (dims, fact, customer_360, alerts) → dataplatform.bank_gold.*
 Orchestration: pl_bank_orchestrator → on failure → pl_send_alert → Logic App email
```

---
## Step 1 — Storage ✅ (done)
Upload `customers_raw.csv` and `accounts_raw.csv` to `landingfinserve/bank/`. Transactions are on your laptop (Step 3).

## Step 2 — Azure SQL (Query editor, Entra login)
Run in order: `sql/00_control_framework.sql` → `sql/bank/01_bank_source.sql`. Then:
```sql
CREATE USER [adf-finserve-rohit] FROM EXTERNAL PROVIDER;      -- skip if it says "already exists"
ALTER ROLE db_datareader ADD MEMBER [adf-finserve-rohit];
ALTER ROLE db_datawriter ADD MEMBER [adf-finserve-rohit];
ALTER ROLE db_ddladmin  ADD MEMBER [adf-finserve-rohit];
GRANT EXECUTE ON SCHEMA::ctl  TO [adf-finserve-rohit];
GRANT EXECUTE ON SCHEMA::bank TO [adf-finserve-rohit];
SELECT project, source_type, source_table FROM ctl.ingestion_config;   -- 3 rows: REST_API / AZURE_SQL / ONPREM_SQL
```

## Step 3 — Laptop SQL Server (on-prem)
1. Copy `transactions_raw.csv` to `C:\finserve\`
2. SSMS → `localhost\SQLEXPRESS` (Windows auth, tick *Trust server certificate*) → run `sql/onprem/01_onprem_core_banking.sql` → `rows_loaded = 2020000`
   - "Access is denied" on BULK INSERT → give `Everyone` *Read* on `C:\finserve`, rerun
3. Server → Properties → Security → **SQL Server and Windows Authentication mode** → restart service *SQL Server (SQLEXPRESS)*

## Step 4 — Self-hosted IR ✅ (done: `ir-selfhosted-laptop` connected)
The laptop must be **on and online** whenever a pipeline reads transactions.

## Step 5 — (skipped) REST source
The Self-hosted IR blocks HTTP calls to local/private addresses, so customers are served from Azure SQL instead.
The framework still supports `source_type = REST_API` (`pl_ingest_api_resource`); REST + pagination is demonstrated in Project 2.

## Step 6 — ADF linked services (Manage → Linked services → + New; names must match exactly)
| Name | Type | Settings |
|---|---|---|
| `ls_azuresql` | Azure SQL Database | Managed identity; server `sql-finserve-rohit`, DB `sqldb-finserve-rohit` |
| `ls_adls` | Azure Data Lake Storage Gen2 | Managed identity; account `stfinservedevrohit` |
| `ls_onprem_sqlserver` | SQL Server | IR `ir-selfhosted-laptop`; `localhost\SQLEXPRESS`; DB `CoreBankingDB`; SQL auth `adf_reader` / `Finserve@2026!`; Encrypt *Optional* / trust server cert |
| `ls_databricks` | Azure Databricks | Managed service identity; workspace `dbw-finserve-dev`; existing interactive cluster (create it in Step 9 first, or come back) |
Test connection on each.

## Step 7 — Load pipelines/datasets into ADF from GitHub
1. Create a GitHub repo `azure-data-platform` and upload the whole kit (drag & drop the folder contents in the GitHub web UI).
2. ADF → Manage → **Git configuration** → Configure → GitHub → your repo → collaboration branch `main` → **Root folder `/adf`** → Import existing resources: **Yes** (so the 5 linked services you created go into the repo too).
3. Author tab → you should see folders `00_framework`, `bank`, `ecom` with all pipelines → **Publish**.

## Step 8 — Run it
1. Debug `pl_seed_source_tables` (project = `bank`) → loads customers (101,000) and accounts (150,000) into Azure SQL.
2. Debug `pl_ingest_master` with `project = bank` (laptop on, IR running).
   - Watch the **Switch** send each table to its child pipeline.
   - `SELECT * FROM ctl.vw_latest_runs` → 3 rows SUCCEEDED (customers ≈101k from API, accounts 150k, transactions 2.02M from your laptop).
   - Files in `bronzefinserve/bank/<table>/load_date=.../`
3. **Incremental test:** in Azure SQL `EXEC bank.usp_simulate_daily_accounts;` · in SSMS `EXEC bank.usp_simulate_daily_transactions;` ·
   the API changes by itself every hour. Run `pl_ingest_master` again → only the new/changed rows are copied.
   Run once more immediately → accounts/transactions show **NO_NEW_DATA**.
4. **Failure test:** stop the Integration Runtime service on your laptop → run → transactions FAILED (error logged, watermark unchanged),
   customers & accounts still succeed. Start the service, rerun → it catches up.

## Step 9 — Databricks (Silver / Gold)
1. Cluster: single node, **Dedicated (single user)** access mode, DBR 15.4 LTS+, `Standard_DS3_v2`/`D4s_v3`, auto-terminate 20 min.
2. Catalog → External data → Credentials → **Create credential**: Azure Managed Identity, name `sc_dataplatform`, access connector = resource ID of `ac-finserve-dev`.
   (`ac-finserve-dev` needs **Storage Blob Data Contributor** on `stfinservedevrohit`.)
3. Workspace → Shared → create folder `dataplatform` → import `databricks/common` and `databricks/bank` (keep folder names).
4. Run `common/nb_setup_catalog` once.
5. Run in order: `bank/nb_silver_customers` → `nb_silver_accounts` → `nb_silver_transactions` → `nb_gold_bank`.
   Expected initial load: customers 101,0xx → ~100,0xx (1,000 dupes removed, 1,010 dates repaired) · accounts 150,000 → 148,500 (+1,500 quarantined) · transactions 2,020,000 → 1,970,111 (19,672 dupes, 30,217 quarantined).
   `SELECT * FROM dataplatform.ops.recon_log ORDER BY logged_at DESC`
6. Re-run a silver notebook → nothing new processed (Auto Loader checkpoint = restartability).

## Step 10 — Orchestrate + alert
1. Logic App (Consumption): trigger *When a HTTP request is received* → action *Gmail / Outlook.com – Send email* → save → copy HTTP URL.
2. Debug `pl_bank_orchestrator` with `logic_app_url` (laptop on).
3. Break a notebook path → run → alert email arrives, run marked Failed. Fix → Publish.
4. Trigger `tr_bank_daily` is **Stopped** — start it only for a demo (saves credit).

## Step 11 — Screenshots for GitHub README
pipeline canvases (master with Switch, children), IR connected, `ctl.vw_latest_runs`, `ops.recon_log`, gold query, alert email.

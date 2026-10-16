# Databricks notebook source
# MAGIC %md
# MAGIC # Bank | Bronze → Silver : accounts
# MAGIC | Issue | Volume (initial load) | Handling |
# MAGIC |---|---|---|
# MAGIC | AccountNumber stored as float text `636800771214.0` | all rows | strip `.0`, must be 12 digits |
# MAGIC | AccountNumber blank | 1,500 | **quarantine** (an account without a number cannot be booked) |
# MAGIC | AccountType blank | 1,500 | default `Unknown` (soft issue, row kept) |
# MAGIC | Status casing `ACTIVE`, `CLOSED`, `DORMANT` | ~2,250 | initcap |
# MAGIC | Negative balance | 1,500 | kept, flagged `is_overdrawn` (business decides, not the pipeline) |
# MAGIC | CustomerID not (yet) in silver.customers | 0 initially, ~2% in daily changes | kept, flagged `is_orphan_customer` → late-arriving dimension, Gold creates an *inferred* customer |

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

PROJECT, ENTITY = "bank", "accounts"
TARGET = table_name("bank_silver", "accounts")
TARGET_PATH = lake_path("silver", "bank/accounts")
CUSTOMERS = table_name("bank_silver", "customers")


def transform(df):
    df = trim_all_strings(df)
    acct_no = F.regexp_replace(F.col("AccountNumber").cast("string"), r"\.0+$", "")
    return df.select(
        F.col("AccountID").cast("int").alias("account_id"),
        F.col("CustomerID").cast("int").alias("customer_id"),
        acct_no.alias("account_number"),
        F.coalesce(F.initcap("AccountType"), F.lit("Unknown")).alias("account_type"),
        F.col("OpenDate").cast("date").alias("open_date"),
        F.col("Balance").cast("decimal(18,2)").alias("balance"),
        (F.col("Balance") < 0).alias("is_overdrawn"),
        F.initcap("Status").alias("status"),
        F.col("ModifiedDate").cast("timestamp").alias("modified_date"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file"),
    )


DQ_RULES = {
    "account_id_not_null": F.col("account_id").isNotNull(),
    "account_number_12_digits": F.col("account_number").rlike(r"^\d{12}$"),
    "status_allowed": F.col("status").isin("Active", "Closed", "Dormant"),
}
BUSINESS_COLS = ["customer_id", "account_number", "account_type", "open_date", "balance", "status", "modified_date"]


def process_batch(batch_df, batch_id):
    bronze_rows = batch_df.count()
    if bronze_rows == 0:
        return
    valid, bad = apply_dq_rules(transform(batch_df), DQ_RULES)
    quarantined = write_quarantine(bad, PROJECT, ENTITY)
    valid = valid.cache()
    latest = dedupe_latest(valid, ["account_id"], [F.col("modified_date").desc(), F.col("_ingest_ts").desc()])
    deduped = latest.count()
    # late-arriving dimension check: broadcast the (distinct) key list of the parent table
    cust_keys = spark.table(CUSTOMERS).select("customer_id").distinct().select("customer_id", F.lit(False).alias("is_orphan_customer"))
    latest = (latest.join(F.broadcast(cust_keys), "customer_id", "left")
                    .withColumn("is_orphan_customer", F.coalesce("is_orphan_customer", F.lit(True))))
    latest = add_audit_columns(latest)
    to_merge = latest.count()          # must equal `deduped`; a fan-out join would break the recon below
    dupes = valid.count() - deduped
    metrics = merge_upsert(latest, TARGET, TARGET_PATH, ["account_id"],
                           BUSINESS_COLS + ["is_orphan_customer"], "modified_date")
    valid.unpersist()
    log_recon(PROJECT, ENTITY, batch_id, bronze_rows, dupes, quarantined, to_merge, metrics)


process_bronze_incremental(PROJECT, ENTITY, process_batch)

# COMMAND ----------

# Re-check orphans every run: customers that arrived late resolve the flag (self-healing)
(DeltaTable.forName(spark, TARGET).alias("t")
    .merge(spark.table(CUSTOMERS).select("customer_id").alias("s"),
           "t.customer_id = s.customer_id AND t.is_orphan_customer = true")
    .whenMatchedUpdate(set={"is_orphan_customer": "false"})
    .execute())
spark.sql(f"SELECT account_type, status, count(*) n, sum(cast(is_orphan_customer as int)) orphans FROM {TARGET} GROUP BY ALL ORDER BY n DESC").show()

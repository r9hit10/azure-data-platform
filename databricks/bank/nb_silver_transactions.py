# Databricks notebook source
# MAGIC %md
# MAGIC # Bank | Bronze → Silver : transactions  (largest table, 2M+ rows)
# MAGIC | Issue | Volume (initial load) | Handling |
# MAGIC |---|---|---|
# MAGIC | Duplicate TransactionID | 20,000 (19,636 exact, 364 differ only in status casing) | dedupe after standardising, keep latest |
# MAGIC | Amount ≤ 0 | 30,217 | **quarantine** (a posted transaction must move money) |
# MAGIC | TransactionDate missing | 20,206 | impute from CreatedDate, flag `is_txn_date_imputed` |
# MAGIC | Status casing `SUCCESS/FAILED/PENDING` | ~20k | initcap |
# MAGIC | Type not Debit/Credit | 0 now | **quarantine** (rule exists for future feeds) |
# MAGIC
# MAGIC **Performance choices** – Delta table uses *liquid clustering* on `(txn_date, account_id)` instead of
# MAGIC `PARTITION BY txn_date`: 2M rows over ~1,000 days would create ~1,000 tiny partitions (small-file problem).
# MAGIC The MERGE key is `transaction_id`; the batch is deduped *before* the MERGE so each target row matches at most one source row.

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

PROJECT, ENTITY = "bank", "transactions"
TARGET = table_name("bank_silver", "transactions")
TARGET_PATH = lake_path("silver", "bank/transactions")
ACCOUNTS = table_name("bank_silver", "accounts")


def transform(df):
    df = trim_all_strings(df)
    txn_ts = F.col("TransactionDate").cast("timestamp")
    created = F.col("CreatedDate").cast("timestamp")
    amount = F.col("Amount").cast("decimal(18,2)")
    ttype = F.initcap("TransactionType")
    return df.select(
        F.col("TransactionID").cast("long").alias("transaction_id"),
        F.col("AccountID").cast("int").alias("account_id"),
        ttype.alias("transaction_type"),
        amount.alias("amount"),
        F.when(ttype == "Debit", -amount).otherwise(amount).alias("signed_amount"),
        F.coalesce(txn_ts, created).alias("transaction_ts"),
        F.to_date(F.coalesce(txn_ts, created)).alias("txn_date"),
        txn_ts.isNull().alias("is_txn_date_imputed"),
        F.initcap("Status").alias("status"),
        created.alias("created_date"),
        F.col("ModifiedDate").cast("timestamp").alias("modified_date"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file"),
    )


DQ_RULES = {
    "transaction_id_not_null": F.col("transaction_id").isNotNull(),
    "amount_positive": F.col("amount") > 0,
    "type_debit_or_credit": F.col("transaction_type").isin("Debit", "Credit"),
    "status_allowed": F.col("status").isin("Success", "Failed", "Pending"),
    "has_transaction_ts": F.col("transaction_ts").isNotNull(),
}
BUSINESS_COLS = ["account_id", "transaction_type", "amount", "transaction_ts", "status", "modified_date"]


def process_batch(batch_df, batch_id):
    batch_df = batch_df.cache()
    bronze_rows = batch_df.count()
    if bronze_rows == 0:
        return
    valid, bad = apply_dq_rules(transform(batch_df), DQ_RULES)
    quarantined = write_quarantine(bad, PROJECT, ENTITY)
    valid = valid.cache()
    latest = dedupe_latest(valid, ["transaction_id"], [F.col("modified_date").desc(), F.col("_ingest_ts").desc()])
    deduped = latest.count()
    # late-arriving dimension check: broadcast the (distinct) key list of the parent table
    acct_keys = spark.table(ACCOUNTS).select("account_id").distinct().select("account_id", F.lit(False).alias("is_orphan_account"))
    latest = (latest.join(F.broadcast(acct_keys), "account_id", "left")
                    .withColumn("is_orphan_account", F.coalesce("is_orphan_account", F.lit(True))))
    latest = add_audit_columns(latest)
    to_merge = latest.count()          # must equal `deduped`; a fan-out join would break the recon below
    dupes = valid.count() - deduped
    metrics = merge_upsert(latest, TARGET, TARGET_PATH, ["transaction_id"], BUSINESS_COLS, "modified_date",
                           cluster_by=["txn_date", "account_id"])
    valid.unpersist(); batch_df.unpersist()
    log_recon(PROJECT, ENTITY, batch_id, bronze_rows, dupes, quarantined, to_merge, metrics)


process_bronze_incremental(PROJECT, ENTITY, process_batch)

# COMMAND ----------

if IS_DATABRICKS:
    spark.sql(f"OPTIMIZE {TARGET}")          # compacts small files written by daily merges (clustering applied)
spark.sql(f"SELECT status, transaction_type, count(*) n, round(sum(amount),2) total FROM {TARGET} GROUP BY ALL ORDER BY n DESC").show()

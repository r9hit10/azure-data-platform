# Databricks notebook source
# MAGIC %md
# MAGIC # Bank | Bronze → Silver : customers  (source: CRM REST API → all columns arrive as strings)
# MAGIC **Issues found in the raw data (profiling) and how they are handled**
# MAGIC | Issue | Volume (initial load) | Handling |
# MAGIC |---|---|---|
# MAGIC | Duplicate CustomerID | 1,000 ids (855 exact copies, 145 conflicting versions) | keep latest `ModifiedDate`; on a tie keep the most complete row |
# MAGIC | ModifiedDate earlier than CreatedDate | 1,010 rows (always exactly 30 days early → systematic source bug) | **repair** to CreatedDate + flag `is_modified_date_repaired`. Quarantining would orphan ~2,400 accounts |
# MAGIC | Status variants `A`, `ACTIVE`, `active`, `InActive` | ~1,000 | map to `Active` / `Inactive` |
# MAGIC | Email upper-case / blank / invalid | 1,515 blank, ~1k invalid | lower-case; `is_email_valid` flag (not dropped: still a valid customer) |
# MAGIC | Phone in 6+ formats with extensions | all | strip to 10 digits, keep extension separately |
# MAGIC | Names with extra spaces / casing | some | trim + initcap |

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

PROJECT, ENTITY = "bank", "customers"
TARGET = table_name("bank_silver", "customers")
TARGET_PATH = lake_path("silver", "bank/customers")

STATUS_MAP = {"active": "Active", "a": "Active", "inactive": "Inactive", "i": "Inactive"}
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"


def transform(df):
    df = trim_all_strings(df)
    status_map = F.create_map(*[F.lit(x) for kv in STATUS_MAP.items() for x in kv])
    phone_ext = F.regexp_extract("Phone", r"x(\d+)$", 1)
    digits = F.regexp_replace(F.regexp_replace("Phone", r"x\d+$", ""), r"\D", "")
    digits = F.when((F.length(digits) == 11) & digits.startswith("1"), F.substring(digits, 2, 10)).otherwise(digits)
    email = F.lower("Email")
    return df.select(
        F.col("CustomerID").cast("int").alias("customer_id"),
        F.initcap("FirstName").alias("first_name"),
        F.initcap("LastName").alias("last_name"),
        email.alias("email"),
        F.when(email.isNull(), None).otherwise(email.rlike(EMAIL_RE)).alias("is_email_valid"),
        F.when(F.length(digits) == 10, digits).alias("phone"),
        F.when(phone_ext != "", phone_ext).alias("phone_ext"),
        F.col("City").alias("city"),
        F.col("State").alias("state"),
        F.col("CreatedDate").cast("timestamp").alias("created_date"),
        F.greatest(F.col("ModifiedDate").cast("timestamp"), F.col("CreatedDate").cast("timestamp")).alias("modified_date"),
        (F.col("ModifiedDate").cast("timestamp") < F.col("CreatedDate").cast("timestamp")).alias("is_modified_date_repaired"),
        F.coalesce(status_map[F.lower("Status")], F.lit("Unknown")).alias("status"),
        F.col("Status").alias("status_raw"),
        F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
        F.col("_source_file"),
    )


DQ_RULES = {
    "customer_id_not_null": F.col("customer_id").isNotNull(),
    "has_modified_date": F.col("modified_date").isNotNull(),
    "name_present": F.col("first_name").isNotNull() & F.col("last_name").isNotNull(),
}
BUSINESS_COLS = ["first_name", "last_name", "email", "phone", "phone_ext", "city", "state", "status", "modified_date"]


def process_batch(batch_df, batch_id):
    bronze_rows = batch_df.count()
    if bronze_rows == 0:
        return
    clean = transform(batch_df)
    valid, bad = apply_dq_rules(clean, DQ_RULES)
    quarantined = write_quarantine(bad, PROJECT, ENTITY)
    valid = valid.cache()
    latest = dedupe_latest(valid, ["customer_id"], [F.col("modified_date").desc(),
                                                     completeness_score(BUSINESS_COLS).desc(),
                                                     F.col("_ingest_ts").desc()])
    latest = add_audit_columns(latest)
    to_merge = latest.count()
    dupes = valid.count() - to_merge
    metrics = merge_upsert(latest, TARGET, TARGET_PATH, ["customer_id"], BUSINESS_COLS, "modified_date")
    valid.unpersist()
    log_recon(PROJECT, ENTITY, batch_id, bronze_rows, dupes, quarantined, to_merge, metrics)


process_bronze_incremental(PROJECT, ENTITY, process_batch)

# COMMAND ----------

display_df = spark.table(TARGET)
print(f"{TARGET}: {display_df.count():,} rows")
display_df.groupBy("status").count().show()

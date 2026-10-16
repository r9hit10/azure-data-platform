# Databricks notebook source
# MAGIC %md
# MAGIC # nb_utils — reusable framework functions (used by all 3 projects)
# MAGIC | Function | What it does |
# MAGIC |---|---|
# MAGIC | `process_bronze_incremental` | Auto Loader over Bronze files + checkpoint → only new files are processed (restartable, exactly-once) |
# MAGIC | `dedupe_latest` | keeps the latest version of each key (ModifiedDate, then completeness score, then ingest time) |
# MAGIC | `apply_dq_rules` | splits a DataFrame into valid rows and quarantined rows with the failed rule names |
# MAGIC | `merge_upsert` | Delta MERGE (insert new, update only if newer and actually changed) |
# MAGIC | `scd2_merge` | Delta SCD Type 2 (expire old version, insert new version) |
# MAGIC | `log_recon` | writes a reconciliation row to `ops.recon_log` and fails the job if counts don't tie out |

# COMMAND ----------

# MAGIC %run ./nb_config

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

OPS_SCHEMA = "ops"


# ---------------------------------------------------------------- generic helpers
def add_audit_columns(df: DataFrame) -> DataFrame:
    return (df.withColumn("_silver_run_id", F.lit(RUN_ID))
              .withColumn("_silver_processed_ts", F.current_timestamp()))


def trim_all_strings(df: DataFrame) -> DataFrame:
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))
    return df


def completeness_score(cols):
    """Number of non-null columns -> tie-breaker when two versions share the same ModifiedDate."""
    return reduce(lambda a, b: a + b, [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in cols])


def dedupe_latest(df: DataFrame, keys, order_cols) -> DataFrame:
    """Keep one row per key. order_cols = list of Column expressions, e.g. [F.col('modified_date').desc()]."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_dq_rules(df: DataFrame, rules: dict) -> (DataFrame, DataFrame):
    """rules = {"rule_name": <Column that is TRUE when the row is GOOD>}
    Returns (valid_df, quarantine_df). Quarantine rows carry the list of failed rules."""
    failed = F.array_compact(F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]))
    tagged = df.withColumn("_dq_failed_rules", failed)
    valid = tagged.filter(F.size("_dq_failed_rules") == 0).drop("_dq_failed_rules")
    bad = tagged.filter(F.size("_dq_failed_rules") > 0)
    return valid, bad


def row_hash(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), 256)


# ---------------------------------------------------------------- table writers
def table_exists(full_name: str) -> bool:
    return spark.catalog.tableExists(full_name)


def create_external_table_if_missing(df: DataFrame, full_name: str, path: str, cluster_by=None):
    if table_exists(full_name):
        return False
    (df.limit(0).write.format("delta").mode("overwrite").option("path", path).saveAsTable(full_name))
    if cluster_by:
        try:   # liquid clustering (DBR 13.3+ / Delta 3.1+) - replaces partitioning + ZORDER for most tables
            spark.sql(f"ALTER TABLE {full_name} CLUSTER BY ({', '.join(cluster_by)})")
        except Exception as e:
            print(f"Liquid clustering not applied: {str(e)[:120]}")
    return True


def merge_upsert(df: DataFrame, full_name: str, path: str, keys, change_cols, ts_col: str, cluster_by=None) -> dict:
    """Idempotent upsert:
       - insert keys that don't exist
       - update only when the source version is newer-or-equal AND the business columns actually changed
    Re-running the same batch therefore changes nothing (safe restart)."""
    df = df.withColumn("_row_hash", row_hash(change_cols))
    create_external_table_if_missing(df, full_name, path, cluster_by)
    tgt = DeltaTable.forName(spark, full_name)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (tgt.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition=f"s.{ts_col} >= t.{ts_col} AND s._row_hash <> t._row_hash")
        .whenNotMatchedInsertAll()
        .execute())
    m = tgt.history(1).select("operationMetrics").collect()[0][0] or {}
    return {"inserted": int(m.get("numTargetRowsInserted", 0)), "updated": int(m.get("numTargetRowsUpdated", 0))}


def scd2_merge(df: DataFrame, full_name: str, path: str, key: str, tracked_cols, ts_col: str,
               first_effective_from: str = None) -> dict:
    """SCD Type 2 with the 'staged union' trick (single MERGE):
       - changed rows appear twice in the staging set: once with merge_key=key (to EXPIRE the current row)
         and once with merge_key=NULL (forces an INSERT of the new version).
       - first_effective_from: the FIRST version of a brand-new key is valid from this date (e.g. '1900-01-01'),
         so facts older than the customer's first change still find a dimension row in point-in-time joins."""
    src = (df.withColumn("_row_hash", row_hash(tracked_cols))
             .withColumn("effective_from", F.col(ts_col).cast("timestamp"))
             .withColumn("effective_to", F.lit(None).cast("timestamp"))
             .withColumn("is_current", F.lit(True)))
    create_external_table_if_missing(src, full_name, path)
    tgt = DeltaTable.forName(spark, full_name)
    if first_effective_from:
        existing_keys = tgt.toDF().select(key).distinct().withColumn("_exists", F.lit(True))
        src = (src.join(existing_keys, key, "left")
                  .withColumn("effective_from", F.when(F.col("_exists").isNull(), F.lit(first_effective_from).cast("timestamp"))
                                                  .otherwise(F.col("effective_from")))
                  .drop("_exists"))
    current = tgt.toDF().filter("is_current = true").select(key, F.col("_row_hash").alias("_tgt_hash"))
    changed = (src.join(current, key, "inner").filter(F.col("_row_hash") != F.col("_tgt_hash")).drop("_tgt_hash"))
    staged = (changed.withColumn("_merge_key", F.lit(None).cast(src.schema[key].dataType))      # new versions -> insert
              .unionByName(src.withColumn("_merge_key", F.col(key))))                           # all rows -> match on key
    (tgt.alias("t").merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(condition="t._row_hash <> s._row_hash",
                           set={"is_current": "false", "effective_to": "s.effective_from"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in src.columns})
        .execute())
    m = tgt.history(1).select("operationMetrics").collect()[0][0] or {}
    return {"inserted": int(m.get("numTargetRowsInserted", 0)), "updated": int(m.get("numTargetRowsUpdated", 0))}


def write_quarantine(bad_df: DataFrame, project: str, entity: str):
    if bad_df.isEmpty():
        return 0
    n = bad_df.count()
    (bad_df.withColumn("_quarantine_run_id", F.lit(RUN_ID)).withColumn("_quarantine_ts", F.current_timestamp())
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .save(lake_path("silver", f"_quarantine/{project}/{entity}")))
    return n


def write_gold(df: DataFrame, schema: str, table: str, project: str):
    """Gold tables are small aggregates -> full rebuild each run (simple, idempotent)."""
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .option("path", lake_path("gold", f"{project}/{table}")).saveAsTable(table_name(schema, table)))
    print(f"gold {schema}.{table}: {spark.table(table_name(schema, table)).count():,} rows")


# ---------------------------------------------------------------- reconciliation / audit
def log_recon(project, entity, batch_id, bronze_rows, duplicates_removed, quarantined, merged_input, merge_metrics,
              fail_on_mismatch=True):
    status = "PASS" if bronze_rows == duplicates_removed + quarantined + merged_input else "FAIL"
    row = [(RUN_ID, project, entity, int(batch_id), int(bronze_rows), int(duplicates_removed), int(quarantined),
            int(merged_input), merge_metrics.get("inserted", 0), merge_metrics.get("updated", 0), status)]
    cols = "run_id string, project string, entity string, batch_id long, bronze_rows long, duplicates_removed long, " \
           "quarantined_rows long, rows_to_merge long, rows_inserted long, rows_updated long, status string"
    (spark.createDataFrame(row, cols).withColumn("logged_at", F.current_timestamp())
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .option("path", lake_path("ops", "recon_log")).saveAsTable(table_name(OPS_SCHEMA, "recon_log")))
    print(f"[RECON {status}] {project}.{entity} batch {batch_id}: bronze={bronze_rows:,} = dupes {duplicates_removed:,}"
          f" + quarantined {quarantined:,} + merged {merged_input:,} -> inserted {merge_metrics.get('inserted',0):,},"
          f" updated {merge_metrics.get('updated',0):,}")
    if status == "FAIL" and fail_on_mismatch:
        raise Exception(f"Reconciliation failed for {project}.{entity} batch {batch_id}")


# ---------------------------------------------------------------- incremental Bronze reader
def process_bronze_incremental(project: str, entity: str, process_batch, file_format: str = "parquet",
                               reader_options: dict = None):
    """Reads ONLY files that arrived in Bronze since the last successful run.
    Databricks: Auto Loader (cloudFiles) + checkpoint, trigger(availableNow) = batch job that is restartable:
      if the job dies mid-way the checkpoint is not committed and the same files are re-read next time;
      merge_upsert is idempotent, so re-processing creates no duplicates.
    Local: plain batch read of the folder (for unit testing)."""
    src = lake_path("bronze", f"{project}/{entity}")
    chk = lake_path("ops", f"checkpoints/{project}/{entity}")
    opts = reader_options or {}
    if IS_DATABRICKS:
        stream = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", file_format)
                  .option("cloudFiles.schemaLocation", f"{chk}/_schema")
                  .option("cloudFiles.inferColumnTypes", "true")
                  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                  .options(**opts)
                  .load(src)
                  .withColumn("_source_file", F.col("_metadata.file_path")))
        if "_ingest_ts" not in stream.columns:       # API copies have no ADF additional columns -> use file time
            stream = stream.withColumn("_ingest_ts", F.col("_metadata.file_modification_time"))
        q = (stream.writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", chk)
             .trigger(availableNow=True)
             .start())
        q.awaitTermination()
    else:
        only = os.environ.get("LOCAL_LOAD_DATE")          # emulate "only new files" when testing
        reader = spark.read.format(file_format).options(**opts)
        df = reader.load(f"{src}/load_date={only}" if only else src).withColumn("_source_file", F.col("_metadata.file_path"))
        if "_ingest_ts" not in df.columns:
            df = df.withColumn("_ingest_ts", F.col("_metadata.file_modification_time"))
        process_batch(df, 0)

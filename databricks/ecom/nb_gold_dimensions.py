# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce | Gold dimensions — SCD Type 2 (customer) and SCD Type 1 (product)
# MAGIC **dim_customer (SCD2)** — tracked columns `city, state, segment`: a change closes the current row
# MAGIC (`effective_to`, `is_current=false`) and inserts a new version. Facts join *as of the order time*.
# MAGIC
# MAGIC **Late-arriving customers (inferred members)** — orders can reference a customer the CRM has not sent yet.
# MAGIC 1. we insert a placeholder row (`is_inferred = true`, attributes `Unknown`) so the fact still gets a key;
# MAGIC 2. when the real customer arrives, the placeholder is **updated in place** (no new version — it was never a real state).
# MAGIC
# MAGIC **dim_product (SCD1)** — product attributes from the API are simply overwritten (history not needed for price list).

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

DIM_CUST = table_name("ecom_gold", "dim_customer")
DIM_CUST_PATH = lake_path("gold", "ecom/dim_customer")
SCD2_COLS = ["city", "state", "segment"]
EPOCH = "1900-01-01 00:00:00"

src = (spark.table(table_name("ecom_silver", "customers"))
       .select("customer_id", "first_name", "last_name", "email", "phone", "city", "state", "segment", "updated_at")
       .withColumn("is_inferred", F.lit(False)))

# --- step 1: real customers replacing inferred placeholders (Type-1 overwrite of the placeholder)
if table_exists(DIM_CUST):
    upd_cols = ["first_name", "last_name", "email", "phone", "city", "state", "segment"]
    staged = src.withColumn("_row_hash", row_hash(SCD2_COLS))
    (DeltaTable.forName(spark, DIM_CUST).alias("t")
        .merge(staged.alias("s"), "t.customer_id = s.customer_id AND t.is_inferred = true")
        .whenMatchedUpdate(set={**{c: f"s.{c}" for c in upd_cols}, "is_inferred": "false", "_row_hash": "s._row_hash",
                                "updated_at": "s.updated_at"})
        .execute())
    print("inferred members resolved:", DeltaTable.forName(spark, DIM_CUST).history(1).select("operationMetrics").collect()[0][0])

# --- step 2: SCD2 merge
m = scd2_merge(src, DIM_CUST, DIM_CUST_PATH, "customer_id", SCD2_COLS, "updated_at", first_effective_from=EPOCH)
print("scd2:", m)

# --- step 3: inferred members for customer ids seen in orders but not in the dimension
known = spark.table(DIM_CUST).select("customer_id").distinct()
missing = (spark.table(table_name("ecom_silver", "orders")).select("customer_id").distinct()
           .join(known, "customer_id", "left_anti"))
if not missing.isEmpty():
    dim_cols = spark.table(DIM_CUST).columns
    placeholders = (missing.select(
        "customer_id", F.lit("Unknown").alias("first_name"), F.lit("Unknown").alias("last_name"),
        F.lit(None).cast("string").alias("email"), F.lit(None).cast("string").alias("phone"),
        F.lit("Unknown").alias("city"), F.lit("Unknown").alias("state"), F.lit("Unknown").alias("segment"),
        F.lit(EPOCH).cast("timestamp").alias("updated_at"), F.lit(True).alias("is_inferred"))
        .withColumn("_row_hash", row_hash(SCD2_COLS))
        .withColumn("effective_from", F.lit(EPOCH).cast("timestamp"))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True)))
    placeholders.select(*dim_cols).write.format("delta").mode("append").saveAsTable(DIM_CUST)
    print(f"inferred members inserted: {placeholders.count():,}")

spark.sql(f"""SELECT is_current, is_inferred, count(*) rows, count(distinct customer_id) customers
              FROM {DIM_CUST} GROUP BY ALL ORDER BY 1,2""").show()

# COMMAND ----------

# ---------------- dim_product (SCD1)
prod = spark.table(table_name("ecom_silver", "products")).drop("_source_file", "_silver_run_id", "_silver_processed_ts", "_row_hash")
m = merge_upsert(prod, table_name("ecom_gold", "dim_product"), lake_path("gold", "ecom/dim_product"), ["product_id"],
                 ["title", "category", "brand", "list_price", "discount_pct", "rating", "availability_status"], "_ingest_ts")
print("dim_product:", m)

# ---------------- dim_date (static, rebuilt)
dates = (spark.sql("SELECT explode(sequence(to_date('2023-01-01'), to_date('2027-12-31'))) AS date")
         .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
                 F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
                 F.date_format("date", "MMMM").alias("month_name"), F.dayofweek("date").alias("day_of_week"),
                 F.date_format("date", "EEEE").alias("day_name"), F.dayofweek("date").isin(1, 7).alias("is_weekend")))
write_gold(dates, "ecom_gold", "dim_date", "ecom")

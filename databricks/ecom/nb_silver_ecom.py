# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce | Bronze → Silver — ONE generic notebook, rules per entity (metadata-driven)
# MAGIC ADF calls this notebook in a ForEach with `p_entity` = products, customers, orders, order_items, payments, inventory, returns.
# MAGIC Each entity declares: key(s), change timestamp, a cleansing function, DQ rules and the business columns used for change detection.

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

try:
    dbutils.widgets.text("p_entity", "customers")
    ENTITY = dbutils.widgets.get("p_entity")
except NameError:
    ENTITY = os.environ.get("P_ENTITY", "customers")
PROJECT = "ecom"


def clean_products(df):
    return df.select(F.col("product_id").cast("int"), F.trim("title").alias("title"),
                     F.initcap(F.regexp_replace("category", "-", " ")).alias("category"),
                     F.coalesce(F.trim("brand"), F.lit("Generic")).alias("brand"),
                     F.col("list_price").cast("decimal(12,2)"), F.col("discount_pct").cast("decimal(5,2)"),
                     F.col("rating").cast("decimal(3,2)"), F.col("api_stock").cast("int"), "sku", "availability_status",
                     F.col("_ingest_ts").cast("timestamp"), "_source_file")


def clean_customers(df):
    df = trim_all_strings(df)
    email = F.lower("email")
    return df.select(F.col("customer_id").cast("int"), F.initcap("first_name").alias("first_name"),
                     F.initcap("last_name").alias("last_name"), email.alias("email"),
                     email.rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$").alias("is_email_valid"),
                     F.col("phone").cast("string").alias("phone"), "city", "state", F.initcap("segment").alias("segment"),
                     F.col("created_at").cast("timestamp"), F.col("updated_at").cast("timestamp"),
                     F.col("_ingest_ts").cast("timestamp"), "_source_file")


def clean_orders(df):
    df = trim_all_strings(df)
    return df.select(F.col("order_id").cast("long"), F.col("customer_id").cast("int"),
                     F.col("order_ts").cast("timestamp"), F.to_date("order_ts").alias("order_date"),
                     F.initcap("status").alias("status"), F.initcap("channel").alias("channel"),
                     F.col("updated_at").cast("timestamp"), F.col("_ingest_ts").cast("timestamp"), "_source_file")


def clean_order_items(df):
    qty, price, disc = F.col("quantity").cast("int"), F.col("unit_price").cast("decimal(12,2)"), F.col("discount_pct").cast("decimal(5,2)")
    return df.select(F.col("order_item_id").cast("long"), F.col("order_id").cast("long"), F.col("product_id").cast("int"),
                     qty.alias("quantity"), price.alias("unit_price"), disc.alias("discount_pct"),
                     F.round(qty * price * (1 - disc), 2).cast("decimal(14,2)").alias("line_amount"),
                     F.col("updated_at").cast("timestamp"), F.col("_ingest_ts").cast("timestamp"), "_source_file")


METHOD_MAP = {"upi": "UPI", "card": "Card", "cod": "COD", "netbanking": "NetBanking", "wallet": "Wallet"}


def clean_payments(df):
    m = F.create_map(*[F.lit(x) for kv in METHOD_MAP.items() for x in kv])
    return df.select(F.col("payment_id").cast("long"), F.col("order_id").cast("long"),
                     F.coalesce(m[F.lower(F.trim("method"))], F.lit("Other")).alias("method"),
                     F.col("amount").cast("decimal(14,2)"), F.initcap("status").alias("status"),
                     F.col("paid_ts").cast("timestamp"), F.col("updated_at").cast("timestamp"),
                     F.col("_ingest_ts").cast("timestamp"), "_source_file")


def clean_inventory(df):
    q = F.col("qty_on_hand").cast("int")
    return df.select(F.upper(F.trim("warehouse_id")).alias("warehouse_id"), F.col("product_id").cast("int"),
                     q.alias("qty_on_hand"), (q < 0).alias("is_negative_stock"),
                     F.col("reorder_level").cast("int"), F.col("updated_at").cast("timestamp"),
                     F.col("_ingest_ts").cast("timestamp"), "_source_file")


def clean_returns(df):
    return df.select(F.col("return_id").cast("long"), F.col("order_item_id").cast("long"),
                     F.col("return_ts").cast("timestamp"), F.initcap(F.trim("reason")).alias("reason"),
                     F.col("refund_amount").cast("decimal(14,2)"), F.col("updated_at").cast("timestamp"),
                     F.col("_ingest_ts").cast("timestamp"), "_source_file")


ENTITIES = {
    "products": dict(keys=["product_id"], ts="_ingest_ts", clean=clean_products,
                     rules={"id": F.col("product_id").isNotNull(), "price_positive": F.col("list_price") > 0},
                     cols=["title", "category", "brand", "list_price", "discount_pct", "rating", "availability_status"]),
    "customers": dict(keys=["customer_id"], ts="updated_at", clean=clean_customers,
                      rules={"id": F.col("customer_id").isNotNull(),
                             "segment_known": F.col("segment").isin("Regular", "Silver", "Gold", "Platinum")},
                      cols=["first_name", "last_name", "email", "phone", "city", "state", "segment"]),
    "orders": dict(keys=["order_id"], ts="updated_at", clean=clean_orders,
                   rules={"id": F.col("order_id").isNotNull(), "has_customer": F.col("customer_id").isNotNull(),
                          "order_not_in_future": F.col("order_ts") <= F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
                          "status_known": F.col("status").isin("Delivered", "Shipped", "Cancelled", "Pending", "Returned")},
                   cols=["customer_id", "order_ts", "status", "channel"]),
    "order_items": dict(keys=["order_item_id"], ts="updated_at", clean=clean_order_items,
                        rules={"id": F.col("order_item_id").isNotNull(), "qty_positive": F.col("quantity") > 0,
                               "price_positive": F.col("unit_price") > 0},
                        cols=["order_id", "product_id", "quantity", "unit_price", "discount_pct"]),
    "payments": dict(keys=["payment_id"], ts="updated_at", clean=clean_payments,
                     rules={"id": F.col("payment_id").isNotNull(), "amount_positive": F.col("amount") > 0},
                     cols=["order_id", "method", "amount", "status", "paid_ts"]),
    "inventory": dict(keys=["warehouse_id", "product_id"], ts="updated_at", clean=clean_inventory,
                      rules={"keys": F.col("warehouse_id").isNotNull() & F.col("product_id").isNotNull()},
                      cols=["qty_on_hand", "reorder_level"]),
    "returns": dict(keys=["return_id"], ts="updated_at", clean=clean_returns,
                    rules={"id": F.col("return_id").isNotNull(), "refund_positive": F.col("refund_amount") > 0},
                    cols=["order_item_id", "return_ts", "reason", "refund_amount"]),
}
cfg = ENTITIES[ENTITY]
TARGET = table_name("ecom_silver", ENTITY)
TARGET_PATH = lake_path("silver", f"ecom/{ENTITY}")
print(f"Processing ecom.{ENTITY} -> {TARGET}")

# COMMAND ----------

def process_batch(batch_df, batch_id):
    batch_df = batch_df.cache()
    bronze_rows = batch_df.count()
    if bronze_rows == 0:
        return
    valid, bad = apply_dq_rules(cfg["clean"](batch_df), cfg["rules"])
    quarantined = write_quarantine(bad, PROJECT, ENTITY)
    valid = valid.cache()
    latest = dedupe_latest(valid, cfg["keys"], [F.col(cfg["ts"]).desc(), F.col("_ingest_ts").desc()])
    latest = add_audit_columns(latest)
    to_merge = latest.count()
    dupes = valid.count() - to_merge
    metrics = merge_upsert(latest, TARGET, TARGET_PATH, cfg["keys"], cfg["cols"], cfg["ts"])
    valid.unpersist(); batch_df.unpersist()
    log_recon(PROJECT, ENTITY, batch_id, bronze_rows, dupes, quarantined, to_merge, metrics)


process_bronze_incremental(PROJECT, ENTITY, process_batch)
print(f"{TARGET}: {spark.table(TARGET).count():,} rows")

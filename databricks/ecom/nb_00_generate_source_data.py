# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce | generate source data (run ONCE, ~3 min on a single node)
# MAGIC Writes 7 CSV files to `landing/ecom/`. ADF `pl_seed_source_tables` (project = `ecom`) then loads them into Azure SQL schema `ecom`.
# MAGIC
# MAGIC | file | rows | planted problems (for Silver to handle) |
# MAGIC |---|---|---|
# MAGIC | customers.csv | ~49k | 1% duplicate rows, 2% blank email, segment casing (`gold`, `GOLD`), spaces in names |
# MAGIC | customers_late.csv | 1,000 | customers that already have orders but arrive LATER (late-arriving dimension) |
# MAGIC | orders.csv | 300k | 0.5% duplicates, status casing, 0.1% future order dates |
# MAGIC | order_items.csv | ~750k | 0.2% quantity ≤ 0, product ids that must match the REST API catalog (1–194) |
# MAGIC | payments.csv | ~295k | 1% amount ≠ order total, method casing |
# MAGIC | inventory.csv | 970 | 5 warehouses × 194 products, some negative stock |
# MAGIC | returns.csv | ~25k | refunds larger than the line amount (0.5%) |
# MAGIC Products are **not** generated — they come from the REST API `https://dummyjson.com/products` (ADF pagination).

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

from pyspark.sql import functions as F

N_CUSTOMERS, N_ORDERS, N_PRODUCTS = 50_000, 300_000, 194
LATE_FROM = 49_001                     # customers 49,001..50,000 arrive late
LANDING = lake_path("landing", "ecom")


def pick(arr, seed_col):
    a = F.array(*[F.lit(x) for x in arr])
    return F.element_at(a, (F.abs(F.xxhash64(seed_col)) % len(arr) + 1).cast("int"))


def rnd(col, salt):          # deterministic 0..1 random from a column
    return (F.abs(F.xxhash64(col, F.lit(salt))) % 1_000_000) / 1_000_000.0


def ts_between(col, salt, start, end):
    s, e = F.unix_timestamp(F.lit(start)), F.unix_timestamp(F.lit(end))
    return F.from_unixtime(s + (rnd(col, salt) * (e - s)).cast("long")).cast("timestamp")


FIRST = ["Aarav", "Vivaan", "Aditya", "Ananya", "Diya", "Ishaan", "Kavya", "Rohan", "Saanvi", "Arjun", "Meera",
         "Kabir", "Riya", "Neha", "Rahul", "Pooja", "Siddharth", "Tanvi", "Yash", "Zara"]
LAST = ["Sharma", "Patel", "Iyer", "Nair", "Khan", "Reddy", "Gupta", "Das", "Joshi", "Mehta", "Singh", "Kulkarni"]
CITY = [("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Bengaluru", "Karnataka"), ("Chennai", "Tamil Nadu"),
        ("Hyderabad", "Telangana"), ("Delhi", "Delhi"), ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"),
        ("Jaipur", "Rajasthan"), ("Kochi", "Kerala")]

# ---------------- customers
c = spark.range(1, N_CUSTOMERS + 1).withColumnRenamed("id", "customer_id")
ci = (F.abs(F.xxhash64("customer_id", F.lit("city"))) % len(CITY) + 1).cast("int")
customers = c.select(
    "customer_id",
    F.when(F.col("customer_id") % 97 == 0, F.concat(F.lit("  "), pick(FIRST, "customer_id"), F.lit(" ")))
     .otherwise(pick(FIRST, "customer_id")).alias("first_name"),
    pick(LAST, F.col("customer_id") * 7).alias("last_name"),
    F.when(F.col("customer_id") % 50 == 0, None)
     .otherwise(F.concat(F.lit("cust"), F.col("customer_id"), F.lit("@shopmail.in"))).alias("email"),
    F.concat(F.lit("9"), F.lpad((F.abs(F.xxhash64("customer_id")) % 1_000_000_000).cast("string"), 9, "0")).alias("phone"),
    F.element_at(F.array(*[F.lit(x[0]) for x in CITY]), ci).alias("city"),
    F.element_at(F.array(*[F.lit(x[1]) for x in CITY]), ci).alias("state"),
    pick(["Regular", "Regular", "Regular", "Silver", "Gold", "Platinum", "gold", "SILVER"], F.col("customer_id") * 3).alias("segment"),
    ts_between("customer_id", "created", "2023-01-01 00:00:00", "2026-06-30 00:00:00").alias("created_at"),
)
customers = customers.withColumn("updated_at", F.col("created_at") + F.expr("make_interval(0,0,0, cast(customer_id % 200 as int))"))
dups = customers.filter(F.col("customer_id") % 100 == 1)                    # 1% exact duplicates
customers_all = customers.unionByName(dups)
on_time = customers_all.filter(F.col("customer_id") < LATE_FROM)
late = customers_all.filter(F.col("customer_id") >= LATE_FROM)

# ---------------- orders
o = spark.range(1, N_ORDERS + 1).withColumnRenamed("id", "order_id")
orders = o.select(
    "order_id",
    (F.abs(F.xxhash64("order_id", F.lit("c"))) % N_CUSTOMERS + 1).alias("customer_id"),
    F.when(F.col("order_id") % 1000 == 0, F.lit("2027-03-01 10:00:00").cast("timestamp"))      # bad future date
     .otherwise(ts_between("order_id", "ts", "2024-01-01 00:00:00", "2026-09-30 23:59:59")).alias("order_ts"),
    pick(["Delivered"] * 14 + ["Shipped", "Cancelled", "Pending", "DELIVERED", "delivered", "Returned"], "order_id").alias("status"),
    pick(["Web", "App", "App", "Store"], F.col("order_id") * 5).alias("channel"),
)
orders = orders.withColumn("updated_at", F.col("order_ts") + F.expr("make_interval(0,0,0, cast(order_id % 6 as int))"))
orders_all = orders.unionByName(orders.filter(F.col("order_id") % 200 == 7))

# ---------------- order items (1-4 per order)
items = (orders.select("order_id", "updated_at", (F.abs(F.xxhash64("order_id", F.lit("n"))) % 4 + 1).alias("n"))
         .withColumn("line_no", F.explode(F.sequence(F.lit(1), F.col("n").cast("int")))))
items = items.select(
    (F.col("order_id") * 10 + F.col("line_no")).alias("order_item_id"),
    "order_id",
    (F.abs(F.xxhash64("order_id", "line_no")) % N_PRODUCTS + 1).alias("product_id"),
    F.when((F.col("order_id") + F.col("line_no")) % 500 == 0, F.lit(0))
     .when((F.col("order_id") + F.col("line_no")) % 777 == 0, F.lit(-1))
     .otherwise(F.abs(F.xxhash64("line_no", "order_id")) % 5 + 1).alias("quantity"),
    F.round(5 + rnd(F.col("order_id") * 10 + F.col("line_no"), "p") * 1995, 2).alias("unit_price"),
    F.when(F.col("line_no") == 2, F.lit(0.1)).otherwise(F.lit(0.0)).alias("discount_pct"),
    "updated_at",
)

# ---------------- payments (one per non-cancelled order)
order_total = (items.filter("quantity > 0").groupBy("order_id")
               .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))), 2).alias("total")))
payments = (orders.filter("lower(status) <> 'cancelled'").join(order_total, "order_id")
            .select(F.col("order_id").alias("payment_id"), "order_id",
                    pick(["UPI", "UPI", "Card", "COD", "NetBanking", "Wallet", "upi", "CARD"], "order_id").alias("method"),
                    F.when(F.col("order_id") % 100 == 3, F.round(F.col("total") * 0.9, 2)).otherwise(F.col("total")).alias("amount"),
                    F.when(F.lower("status") == "pending", "Pending").otherwise("Captured").alias("status"),
                    (F.col("order_ts") + F.expr("INTERVAL 5 MINUTES")).alias("paid_ts"),
                    "updated_at"))

# ---------------- inventory
inventory = (spark.range(1, 6).withColumnRenamed("id", "w").crossJoin(spark.range(1, N_PRODUCTS + 1).withColumnRenamed("id", "product_id"))
             .select(F.concat(F.lit("WH-0"), F.col("w")).alias("warehouse_id"), "product_id",
                     (F.abs(F.xxhash64("w", "product_id")) % 500 - 20).alias("qty_on_hand"),
                     (F.abs(F.xxhash64("product_id", "w")) % 50 + 10).alias("reorder_level"),
                     F.lit("2026-09-30 00:00:00").cast("timestamp").alias("updated_at")))

# ---------------- returns (~4% of delivered items)
delivered_items = items.join(orders.filter("lower(status) in ('delivered','returned')").select("order_id", "order_ts"), "order_id")
returns = (delivered_items.filter((F.abs(F.xxhash64("order_item_id", F.lit("r"))) % 100) < 4)
           .select(F.col("order_item_id").alias("return_id"), "order_item_id",
                   (F.col("order_ts") + F.expr("INTERVAL 7 DAYS")).alias("return_ts"),
                   pick(["Damaged", "Wrong size", "Not as described", "Changed mind", "Late delivery"], "order_item_id").alias("reason"),
                   F.when(F.col("order_item_id") % 200 == 0, F.round(F.col("quantity") * F.col("unit_price") * 1.5, 2))
                    .otherwise(F.round(F.col("quantity") * F.col("unit_price"), 2)).alias("refund_amount"),
                   (F.col("order_ts") + F.expr("INTERVAL 8 DAYS")).alias("updated_at")))

# COMMAND ----------

def write_single_csv(df, name):
    """Spark writes a folder of part files; copy the single part file to landing/ecom/<name>.csv"""
    tmp = f"{LANDING}/_tmp_{name}"
    df.coalesce(1).write.mode("overwrite").option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(tmp)
    if IS_DATABRICKS:
        part = [f.path for f in dbutils.fs.ls(tmp) if f.name.startswith("part-")][0]
        dbutils.fs.cp(part, f"{LANDING}/{name}.csv")
        dbutils.fs.rm(tmp, True)
    else:
        import glob, shutil
        shutil.copy(glob.glob(f"{tmp}/part-*.csv")[0], f"{LANDING}/{name}.csv")
        shutil.rmtree(tmp)


for name, df in [("customers", on_time), ("customers_late", late), ("orders", orders_all), ("order_items", items),
                 ("payments", payments), ("inventory", inventory), ("returns", returns)]:
    write_single_csv(df.orderBy(df.columns[0]), name)
    print(f"{name:15s} written")

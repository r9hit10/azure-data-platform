# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce | Gold facts & marts
# MAGIC | table | grain | notes |
# MAGIC |---|---|---|
# MAGIC | `fact_sales` | order line | **point-in-time join** to SCD2 customer (city/segment *at order time*), returns attached |
# MAGIC | `fact_payment_recon` | order | payment amount vs order total → finance reconciliation flag |
# MAGIC | `agg_daily_sales` | day × channel | revenue, orders, units, return value |
# MAGIC | `product_performance` | product | units, revenue, return rate |
# MAGIC | `customer_rfm` | customer | Recency / Frequency / Monetary scores (1–5) |
# MAGIC | `inventory_reorder_alerts` | warehouse × product | stock at or below reorder level |

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

S, G, P = "ecom_silver", "ecom_gold", "ecom"
orders = spark.table(table_name(S, "orders"))
items = spark.table(table_name(S, "order_items"))
returns = spark.table(table_name(S, "returns"))
payments = spark.table(table_name(S, "payments"))
dim_c = spark.table(table_name(G, "dim_customer"))
dim_p = spark.table(table_name(G, "dim_product"))

dim_c_keyed = dim_c.select("customer_id", "effective_from", "effective_to",
                           F.xxhash64("customer_id", "effective_from").alias("customer_sk"),
                           F.col("city").alias("customer_city_at_order"), F.col("segment").alias("customer_segment_at_order"))
ret = returns.groupBy("order_item_id").agg(F.sum("refund_amount").alias("refund_amount"), F.first("reason").alias("return_reason"))

o = orders.select("order_id", "customer_id", "order_ts", "order_date", F.col("status").alias("order_status"), "channel")
fact = (items.join(o, "order_id", "inner")
        .join(dim_c_keyed, (o.customer_id == dim_c_keyed.customer_id) & (F.col("order_ts") >= F.col("effective_from"))
              & (F.col("effective_to").isNull() | (F.col("order_ts") < F.col("effective_to"))), "left")
        .drop(dim_c_keyed.customer_id).drop("effective_from", "effective_to")
        .join(F.broadcast(dim_p.select("product_id", "category", "brand")), "product_id", "left")
        .join(ret, "order_item_id", "left")
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("is_returned", F.col("refund_amount").isNotNull())
        .withColumn("is_refund_over_line", F.col("refund_amount") > F.col("line_amount"))
        .withColumn("net_amount", F.col("line_amount") - F.coalesce(F.least("refund_amount", "line_amount"), F.lit(0)))
        .select("order_item_id", "order_id", "date_key", "order_ts", "customer_id", "customer_sk",
                "customer_city_at_order", "customer_segment_at_order", "product_id", "category", "brand", "channel",
                "order_status", "quantity", "unit_price", "discount_pct", "line_amount", "is_returned",
                "refund_amount", "return_reason", "is_refund_over_line", "net_amount"))
write_gold(fact, G, "fact_sales", P)
orphans = spark.table(table_name(G, "fact_sales")).filter("customer_sk IS NULL").count()
print(f"fact rows without customer key: {orphans}")
assert orphans == 0, "every order line must resolve to a customer version (inferred members guarantee this)"

# COMMAND ----------

order_tot = items.groupBy("order_id").agg(F.sum("line_amount").alias("order_total"))
pay_recon = (orders.filter("status <> 'Cancelled'").select("order_id", "order_date").join(order_tot, "order_id", "left")
             .join(payments.groupBy("order_id").agg(F.sum("amount").alias("paid_amount")), "order_id", "left")
             .withColumn("difference", F.round(F.coalesce("paid_amount", F.lit(0)) - F.coalesce("order_total", F.lit(0)), 2))
             .withColumn("recon_status", F.when(F.col("paid_amount").isNull(), "NOT_PAID")
                                          .when(F.abs("difference") <= 1, "MATCHED").otherwise("MISMATCH")))
write_gold(pay_recon, G, "fact_payment_recon", P)

fs = spark.table(table_name(G, "fact_sales"))
daily = (fs.filter("order_status <> 'Cancelled'").groupBy("date_key", "channel")
         .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"),
              F.sum("line_amount").alias("gross_revenue"), F.sum("net_amount").alias("net_revenue"),
              F.sum(F.coalesce("refund_amount", F.lit(0))).alias("refunds")))
write_gold(daily, G, "agg_daily_sales", P)

prodperf = (fs.groupBy("product_id", "category", "brand")
            .agg(F.sum("quantity").alias("units_sold"), F.sum("net_amount").alias("net_revenue"),
                 F.round(F.avg(F.col("is_returned").cast("int")) * 100, 2).alias("return_rate_pct"))
            .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("net_revenue")))))
write_gold(prodperf, G, "product_performance", P)

as_of = fs.agg(F.max("order_ts")).collect()[0][0]
rfm = (fs.filter("order_status <> 'Cancelled'").groupBy("customer_id")
       .agg(F.datediff(F.lit(as_of), F.max("order_ts")).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"), F.sum("net_amount").alias("monetary"))
       .withColumn("r_score", 6 - F.ntile(5).over(Window.orderBy("recency_days")))
       .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
       .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
       .withColumn("rfm_segment", F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), "Champions")
                                   .when(F.col("r_score") >= 4, "Recent")
                                   .when(F.col("f_score") >= 4, "Loyal - at risk")
                                   .when(F.col("r_score") <= 2, "Hibernating").otherwise("Needs attention")))
write_gold(rfm, G, "customer_rfm", P)

inv = spark.table(table_name(S, "inventory"))
reorder = (inv.filter("qty_on_hand <= reorder_level").join(F.broadcast(dim_p.select("product_id", "title", "category")), "product_id", "left")
           .select("warehouse_id", "product_id", "title", "category", "qty_on_hand", "reorder_level", "is_negative_stock"))
write_gold(reorder, G, "inventory_reorder_alerts", P)

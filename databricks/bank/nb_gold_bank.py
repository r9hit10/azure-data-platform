# Databricks notebook source
# MAGIC %md
# MAGIC # Bank | Silver → Gold  (star schema + business marts)
# MAGIC | Gold table | Grain | Used for |
# MAGIC |---|---|---|
# MAGIC | `dim_customer` | 1 row / customer (+ *inferred* members for late-arriving ids) | slicing by customer attributes |
# MAGIC | `dim_account` | 1 row / account | account attributes |
# MAGIC | `fact_transactions` | 1 row / successful-or-pending transaction | all transaction analytics |
# MAGIC | `agg_daily_txn_summary` | date × type × status | daily volume dashboard |
# MAGIC | `customer_360` | 1 row / customer | relationship managers: balances, activity, segment |
# MAGIC | `high_value_txn_alerts` | flagged transactions | AML-style monitoring (amount > 3× account average and > 50,000) |

# COMMAND ----------

# MAGIC %run ../common/nb_utils

# COMMAND ----------

S, G, P = "bank_silver", "bank_gold", "bank"
cust = spark.table(table_name(S, "customers"))
acct = spark.table(table_name(S, "accounts"))
txn = spark.table(table_name(S, "transactions"))

# ---------- dim_customer with inferred members (accounts whose customer has not arrived yet)
inferred = (acct.join(cust.select("customer_id"), "customer_id", "left_anti")
                .select("customer_id").distinct()
                .withColumn("first_name", F.lit("Unknown")).withColumn("last_name", F.lit("Unknown"))
                .withColumn("is_inferred", F.lit(True)))
dim_customer = (cust.select("customer_id", "first_name", "last_name", "email", "is_email_valid", "phone",
                            "city", "state", "status", "created_date")
                    .withColumn("is_inferred", F.lit(False))
                    .unionByName(inferred, allowMissingColumns=True)
                    .withColumn("customer_sk", F.xxhash64("customer_id")))
write_gold(dim_customer, G, "dim_customer", P)

dim_account = acct.select("account_id", "customer_id", "account_number", "account_type", "open_date",
                          "status", "balance", "is_overdrawn").withColumn("account_sk", F.xxhash64("account_id"))
write_gold(dim_account, G, "dim_account", P)

# COMMAND ----------

fact = (txn.filter("status <> 'Failed'")
           .join(acct.select("account_id", "customer_id"), "account_id", "left")
           .select("transaction_id", "account_id", "customer_id", "txn_date", "transaction_ts",
                   "transaction_type", "status", "amount", "signed_amount", "is_txn_date_imputed")
           .withColumn("account_sk", F.xxhash64("account_id"))
           .withColumn("customer_sk", F.xxhash64("customer_id")))
write_gold(fact, G, "fact_transactions", P)

daily = (txn.groupBy("txn_date", "transaction_type", "status")
            .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("total_amount"),
                 F.avg("amount").cast("decimal(18,2)").alias("avg_amount")))
write_gold(daily, G, "agg_daily_txn_summary", P)

# COMMAND ----------

as_of = txn.agg(F.max("txn_date")).collect()[0][0]
recent = (txn.filter((F.col("status") == "Success") & (F.col("txn_date") > F.date_sub(F.lit(as_of), 90)))
             .join(acct.select("account_id", "customer_id"), "account_id")
             .groupBy("customer_id")
             .agg(F.count("*").alias("txn_count_90d"),
                  F.sum(F.when(F.col("transaction_type") == "Debit", F.col("amount"))).alias("debit_90d"),
                  F.sum(F.when(F.col("transaction_type") == "Credit", F.col("amount"))).alias("credit_90d"),
                  F.max("txn_date").alias("last_txn_date")))
acct_agg = (acct.groupBy("customer_id")
                .agg(F.count("*").alias("num_accounts"),
                     F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).alias("active_accounts"),
                     F.sum("balance").alias("total_balance")))
c360 = (cust.select("customer_id", "first_name", "last_name", "city", "state", "status")
            .join(acct_agg, "customer_id", "left").join(recent, "customer_id", "left")
            .fillna(0, ["num_accounts", "active_accounts", "txn_count_90d"])
            .withColumn("segment", F.when(F.col("total_balance") >= 1_000_000, "Premium")
                                    .when(F.col("total_balance") >= 250_000, "Gold")
                                    .when(F.col("total_balance") > 0, "Standard").otherwise("No balance"))
            .withColumn("is_dormant_90d", F.col("txn_count_90d") == 0)
            .withColumn("as_of_date", F.lit(as_of)))
write_gold(c360, G, "customer_360", P)

# COMMAND ----------

w = Window.partitionBy("account_id")
alerts = (txn.filter("status = 'Success'")
             .withColumn("acct_avg_amount", F.avg("amount").over(w))
             .withColumn("acct_txn_count", F.count("*").over(w))
             .filter((F.col("amount") > 3 * F.col("acct_avg_amount")) & (F.col("amount") > 50000) & (F.col("acct_txn_count") >= 5))
             .select("transaction_id", "account_id", "txn_date", "transaction_type", "amount",
                     F.round("acct_avg_amount", 2).alias("acct_avg_amount"),
                     F.round(F.col("amount") / F.col("acct_avg_amount"), 1).alias("x_times_avg")))
write_gold(alerts, G, "high_value_txn_alerts", P)

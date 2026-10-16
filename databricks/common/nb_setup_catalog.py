# Databricks notebook source
# MAGIC %md
# MAGIC # nb_setup_catalog — run ONCE after creating the storage credential in Catalog Explorer
# MAGIC **Before running:** Catalog → External data → Credentials → *Create credential*
# MAGIC - Type: Azure Managed Identity, name **`sc_dataplatform`**
# MAGIC - Access connector ID: the resource ID of `ac-finserve-dev` (copy from the Access Connector overview page)
# MAGIC
# MAGIC Then run this notebook. It creates one external location per container, the catalog and all schemas.

# COMMAND ----------

# MAGIC %run ./nb_config

# COMMAND ----------

CREDENTIAL = "sc_dataplatform"
for container in CONTAINERS:          # logical names -> el_landing, el_bronze ... pointing at *finserve containers
    spark.sql(f"""CREATE EXTERNAL LOCATION IF NOT EXISTS el_{container}
                  URL '{lake_path(container)}' WITH (STORAGE CREDENTIAL {CREDENTIAL})
                  COMMENT '{container} layer'""")
    print("external location ok:", container)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{lake_path('ops', 'uc_managed')}'")
for schema in ["ops",
               "bank_silver", "bank_gold",
               "ecom_silver", "ecom_gold",
               "scm_bronze", "scm_silver", "scm_gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print("schema ok:", schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN dataplatform

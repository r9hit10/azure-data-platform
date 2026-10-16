# Databricks notebook source
# MAGIC %md
# MAGIC # nb_config — single place for environment settings
# MAGIC Every project notebook starts with `%run ../common/nb_utils` (which runs this file first).
# MAGIC **Edit `STORAGE_ACCOUNT` once** and everything else follows.

# COMMAND ----------

import os

STORAGE_ACCOUNT = "stfinservedevrohit"      # <-- EDIT: your ADLS Gen2 account name
CATALOG = "dataplatform"                  # Unity Catalog catalog created by nb_setup_catalog

# logical layer -> physical container name in the storage account
CONTAINERS = {
    "landing": "landingfinserve",
    "bronze":  "bronzefinserve",
    "silver":  "silverfinserve",
    "gold":    "goldfinserve",
    "ops":     "opsfinserve",
}

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
LOCAL_LAKE = os.environ.get("LOCAL_LAKE", "/tmp/lake")   # only used for local unit testing

if not IS_DATABRICKS:
    CATALOG = "spark_catalog"


def lake_path(container: str, relative: str = "") -> str:
    """abfss path on Azure, local folder when testing on a laptop."""
    relative = relative.strip("/")
    container = CONTAINERS.get(container, container)
    if IS_DATABRICKS:
        return f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{relative}"
    return f"{LOCAL_LAKE}/{container}/{relative}"


def table_name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


# Runtime parameters passed by the ADF Databricks Notebook activity (baseParameters)
try:
    dbutils.widgets.text("p_run_id", "manual")
    dbutils.widgets.text("p_load_date", "")
    RUN_ID = dbutils.widgets.get("p_run_id")
    LOAD_DATE = dbutils.widgets.get("p_load_date")
except NameError:          # local run, no dbutils
    RUN_ID, LOAD_DATE = os.environ.get("RUN_ID", "local-run"), ""

# Sensible Spark defaults (AQE is on by default in DBR, set explicitly so it is visible in interviews)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
if IS_DATABRICKS:
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")   # fewer, bigger files
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print(f"RUN_ID={RUN_ID} | CATALOG={CATALOG} | bronze root={lake_path('bronze')}")

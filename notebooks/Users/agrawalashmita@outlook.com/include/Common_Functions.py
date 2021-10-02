# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date",current_timestamp())
  return output_df

# COMMAND ----------

def mount_adls(container_name,storage_account_name):
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)

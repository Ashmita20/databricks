# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/race")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_filter_df = race_df.filter("year = 2019 and round <= 5")

# COMMAND ----------

race_filter_df = race_df.filter((race_df["year"]==2019) & (race_df["round"] <= 5))

# COMMAND ----------

display(race_filter_df)

# COMMAND ----------



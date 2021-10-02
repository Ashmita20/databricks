# Databricks notebook source
# MAGIC %md
# MAGIC ### Create Temp View

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = "2020"

# COMMAND ----------

v_race_year = 2020
dbutils.widgets.text("race_year","2019")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.get("race_year")

# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from v_race_results where race_year ="+dbutils.widgets.get("race_year"))

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------



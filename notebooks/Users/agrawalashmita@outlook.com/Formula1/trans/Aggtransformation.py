# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year IN (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name == 'Lewis Hamilton'").select(sum("points"),countDistinct("race_name"))\
.withColumnRenamed("sum(points)","total points") \
.withColumnRenamed("count(DISTINCT race_name)","race_count") \
.show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points"),countDistinct("race_name")).show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("race_count")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("race_count"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

demo_grouped_df.withColumn("rank",rank().over(driver_rank_spec)).show()

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------


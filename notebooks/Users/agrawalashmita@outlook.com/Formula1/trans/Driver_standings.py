# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col
driver_standings_df = race_results_df.groupBy("race_year","driver_name","driver_nationality","team").agg(sum("points").alias("total_points"), \
                                                                                                         count(when (col("position") == 1, True)).alias("Wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year == 2020" ))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

driver_rank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("Wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank))

# COMMAND ----------

display(final_df.filter("race_year ==2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings/")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col
constructor_standings_df = race_results_df.groupBy("race_year","team").agg(sum("points").alias("total_points"), \
                                                                                                         count(when (col("position") == 1, True)).alias("Wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank,asc

constructor_rank = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("Wins"))
final_constructor_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank))

# COMMAND ----------

display(final_constructor_df)

# COMMAND ----------

display(final_constructor_df.filter("race_year ==2020"))

# COMMAND ----------

final_constructor_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/contructor_standings/")

# COMMAND ----------



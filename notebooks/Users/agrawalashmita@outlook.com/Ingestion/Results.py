# Databricks notebook source
dbutils.fs.ls("/mnt/trialashmita20/raw/")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
qualify_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                 StructField("raceId",IntegerType(),True),
                                 StructField("driveId",IntegerType(),True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("position",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                  StructField("q1",StringType(),True),
                                   StructField("q2",StringType(),True),
                                   StructField("q3",StringType(),True)])


# COMMAND ----------

results_df  = spark.read.option("inferSchema",True).json("/mnt/trialashmita20/raw/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
results_final_df  = results_df.withColumn("inghestion_date",current_timestamp())

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet("/mnt/trialashmita20/processed/results")

# COMMAND ----------

dbutils.notebook.exit("Success")

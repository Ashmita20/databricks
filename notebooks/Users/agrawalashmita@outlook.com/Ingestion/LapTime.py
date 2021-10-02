# Databricks notebook source
dbutils.fs.ls("/mnt/trialashmita20/raw/")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
laptime_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driveId",IntegerType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("position",StringType(),True),
                                 StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True)])
laptime_df  = spark.read.schema(laptime_schema).csv("/mnt/trialashmita20/raw/lap_times/")

# COMMAND ----------

display(laptime_df)

# COMMAND ----------

laptime_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
laptime_final_df = laptime_df.withColumnRenamed("driverId","driver_Id") \
                    .withColumnRenamed("raceId","race_Id") \
                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(laptime_final_df)

# COMMAND ----------

laptime_final_df.write.mode("overwrite").parquet("/mnt/trialashmita20/processed/laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success")

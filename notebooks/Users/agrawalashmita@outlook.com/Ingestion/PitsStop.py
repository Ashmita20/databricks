# Databricks notebook source
dbutils.fs.ls("/mnt/trialashmita20/raw/")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
pitstop_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driveId",IntegerType(),True),
                                 StructField("stop",StringType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("duration",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True)])
pitstop = spark.read.option("schema",pitstop_schema).option("multiline",True).json("/mnt/trialashmita20/raw/pit_stops.json")

# COMMAND ----------

display(pitstop)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstop_df = pitstop.withColumnRenamed("driverId","driver_Id") \
                    .withColumnRenamed("raceId","race_Id") \
                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

pitstop_df.write.mode("overwrite").partitionBy("race_Id").parquet("/mnt/trialashmita20/processed/pitstops")

# COMMAND ----------

dbutils.notebook.exit("Success")

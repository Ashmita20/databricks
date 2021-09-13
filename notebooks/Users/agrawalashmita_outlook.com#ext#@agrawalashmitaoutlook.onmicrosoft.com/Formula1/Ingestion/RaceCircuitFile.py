# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

race_df = spark.read.option("header",True).option("inferSchema",True).csv("/mnt/trialashmita/raw/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitId",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",StringType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)])

# COMMAND ----------

race_df = spark.read.option("header",True).option("Schema",race_schema).csv("/mnt/trialashmita/raw/races.csv")

# COMMAND ----------

race_select_df = race_df.drop(race_df.url)

# COMMAND ----------

display(race_select_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

race_with_df = race_select_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(race_with_df)

# COMMAND ----------


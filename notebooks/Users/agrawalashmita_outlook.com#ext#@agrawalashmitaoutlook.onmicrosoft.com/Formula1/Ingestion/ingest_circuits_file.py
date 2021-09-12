# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits csv File

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/trialashmita/raw

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType

# COMMAND ----------

circuit_Schema= StructType(fields=[StructField("circuitId" ,IntegerType(),False),
                                   StructField("circuitRef", StringType(),True),
                                   StructField("name" ,StringType(),True),
                                   StructField("location", StringType(),True), 
                                   StructField("country", StringType(),True) ,
                                   StructField("lat", DoubleType(),True) ,
                                   StructField("lng", DoubleType(),True) ,
                                   StructField("alt", IntegerType(),True),
                                   StructField("url", StringType(),True)])

# COMMAND ----------

circuit_df = spark.read.csv("dbfs:/mnt/trialashmita/raw/circuits.csv",header="True",schema=circuit_Schema)
#circuit_df = spark.read.option("header",True).csv("dbfs:/mnt/trialashmita/raw/circuits.csv",header="True")

# COMMAND ----------

type(circuit_df)

# COMMAND ----------

circuit_df.show()

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

circuit_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Select Particular Columns

# COMMAND ----------

circuit_select_df = circuit_df.select(circuit_df.circuitId,circuit_df.circuitRef,circuit_df.name,circuit_df.location,circuit_df.country,circuit_df.lat,circuit_df.lng,circuit_df.alt)

# COMMAND ----------

display(circuit_select_df)
# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Circuits csv File

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

dbutils.widgets.text("p_data_source","testing")
v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/trialashmita20/raw

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

circuit_df = spark.read.csv(f"{raw_folder_path}/circuits.csv",header="True",schema=circuit_Schema)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename Columns of DataFrame

# COMMAND ----------

circuit_renamed_df = circuit_select_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_Ref")\
.withColumnRenamed("name","Name")\
.withColumnRenamed("location","Location")\
.withColumnRenamed("country","Country")\
.withColumnRenamed("lat","Latitude")\
.withColumnRenamed("lng","Longitude")\
.withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new column in Dataframe using With Clause

# COMMAND ----------

# MAGIC %run "../include/Common_Functions"

# COMMAND ----------

circuits_final_df = ingestion_date(circuit_renamed_df)
from pyspark.sql.functions import lit
circuits_final_df = circuits_final_df.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data to Data Lake as Parquet

# COMMAND ----------

circuits_final_df.write.parquet(f"{processed_folder_path}/circuits",mode = "overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/trialashmita/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

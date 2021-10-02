# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Json File

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls("/mnt/trialashmita20/raw")

# COMMAND ----------

constructor_df = spark.read.json("/mnt/trialashmita20/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_schema ="constructorId INT,constructorRef String, name String,nationality String,url String "

# COMMAND ----------

constructor_df = spark.read.option("schema",constructor_schema).json("/mnt/trialashmita20/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import col
constructor_select_df = constructor_df.select(col("constructorId").alias("constructor_Id"),col("constructorRef").alias("constructor_Ref"),col("name").alias("Name"),col("nationality").alias("Nationality"))

# COMMAND ----------

display(constructor_select_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructor_final_df = constructor_select_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

constructor_final_df.write.parquet("/mnt/trialashmita20/processed/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")

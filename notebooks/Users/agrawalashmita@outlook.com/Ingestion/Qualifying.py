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

qualify_df  = spark.read.schema(qualify_schema).option("multiline",True).json("/mnt/trialashmita20/raw/qualifying/")

# COMMAND ----------

display(qualify_df)

# COMMAND ----------

qualify_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualify_final_df = qualify_df.withColumnRenamed("driverId","driver_Id") \
                    .withColumnRenamed("raceId","race_Id") \
                    .withColumnRenamed("constructorId","constructor_Id") \
                    .withColumnRenamed("qualifyingId","qualifying_Id") \
                    .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(qualify_final_df)

# COMMAND ----------

qualify_final_df.write.mode("overwrite").parquet("/mnt/trialashmita20/processed/qualify")

# COMMAND ----------

dbutils.notebook.exit("Success")

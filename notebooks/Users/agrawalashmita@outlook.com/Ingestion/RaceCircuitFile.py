# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

race_df = spark.read.option("header",True).option("inferSchema",True).csv("/mnt/trialashmita20/raw/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField,DateType

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitId",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",DateType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)])

# COMMAND ----------

race_df = spark.read.option("header",True).option("Schema",race_schema).csv("/mnt/trialashmita20/raw/races.csv")

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

##race_select_df = race_df.select(race_df.raceId,race_df.year,race_df.round,race_df.circuitId,race_df.name,race_df.date,race_df.time)
##race_select_df = race_df.select(race_df["raceId"],race_df["year"],race_df["round"],race_df["circuitId"],race_df["name"],race_df["date"],race_df["time"])
race_select_df = race_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))
##race_select_df = race_df.drop(race_df.url)

# COMMAND ----------

display(race_select_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,to_timestamp,lit,concat

# COMMAND ----------

race_with_df = race_select_df.withColumn("ingestion_date",current_timestamp())\
                             .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(race_with_df)

# COMMAND ----------

race_select_df = race_with_df.select(col("raceId").alias("race_id"),col("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

display(race_select_df)

# COMMAND ----------

race_select_df.write.mode("overwrite").partitionBy("year").parquet("/mnt/trialashmita20/processed/race")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/trialashmita20/processed/race

# COMMAND ----------

df = spark.read.parquet("/mnt/trialashmita20/processed/race")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

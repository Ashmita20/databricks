# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading Json File

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls("/mnt/trialashmita20/raw")

# COMMAND ----------

drivers_df = spark.read.json("/mnt/trialashmita20/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,DoubleType,StringType,DateType
name_schema = StructType(fields=[StructField("forename",StringType()),
                                 StructField("surname",StringType())])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("code",IntegerType(),False),
                                 StructField("dob",DateType(),True),
                                 StructField("driverId",IntegerType(),True),
                                 StructField("driverRef",StringType(),True),
                                 StructField("name",name_schema),
                                 StructField("nationality",StringType(),True),
                                  StructField("number",IntegerType(),True)])

# COMMAND ----------

drivers_df = spark.read.option("schema",driver_schema).json("/mnt/trialashmita20/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

###constructor_schema ="constructorId INT,constructorRef String, name String,nationality String,url String "

# COMMAND ----------

###constructor_df = spark.read.option("schema",constructor_schema).json("/mnt/trialashmita20/raw/constructors.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp,concat
driver_with_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                            .withColumnRenamed("driverRef","driver_Ref")\
                            .withColumn("ingestion_date",current_timestamp())\
                            .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

display(driver_with_df)

# COMMAND ----------

driver_final_df = driver_with_df.na.replace(["\N"], ['UU'], 'code')

# COMMAND ----------

driver_final_df = driver_final_df.drop("url")

# COMMAND ----------

driver_final_df.write.mode("overwrite").partitionBy("nationality").parquet("/mnt/trialashmita20/processed/drivers")

# COMMAND ----------

df = spark.read.parquet("/mnt/trialashmita20/processed/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("Location","circuit_location")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality","driver_nationality") 

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/race") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date") \
.withColumnRenamed("year","race_year")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.withColumnRenamed("time","race_time")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
.withColumnRenamed("Name","Team")

# COMMAND ----------

circuit_race_df = race_df.join(circuits_df,race_df.circuit_id == circuits_df.circuit_id,"inner") \
.select(race_df.race_id,race_df.race_name,race_df.race_date,circuits_df.circuit_location,race_df.race_year)

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------

race_results_df = results_df.join(circuit_race_df,circuit_race_df.race_id == results_df.raceId,"inner") \
                  .join(drivers_df,drivers_df.driver_id == results_df.driverId,"inner") \
                  .join(constructors_df,constructors_df.constructor_Id == results_df.constructorId,"inner")                  

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select("race_name","race_year","race_date","driver_name","driver_number","circuit_location","driver_nationality","team","grid","fastestLap","race_time","points","position").withColumn("creation_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year ==2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy("points",ascending=False))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------



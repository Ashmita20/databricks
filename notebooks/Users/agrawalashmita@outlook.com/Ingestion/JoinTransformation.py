# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/race").filter("year = 2019")

# COMMAND ----------

display(race_df)

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"inner").select(circuits_df.circuit_id,circuits_df.Name,race_df.year,race_df.round)

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"left_outer").select(circuits_df.circuit_id,circuits_df.Name,race_df.year,race_df.round)

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"full_outer").select(circuits_df.circuit_id,circuits_df.Name,race_df.year,race_df.round)

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"right_outer").select(circuits_df.circuit_id,circuits_df.Name,race_df.year,race_df.round)

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Semi Join

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"semi").select(circuits_df.circuit_id,circuits_df.Name)

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------

circuit_race_df = circuits_df.join(race_df,circuits_df.circuit_id == race_df.circuit_id,"anti")

# COMMAND ----------

display(circuit_race_df)

# COMMAND ----------

race_circuit_df = race_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuit_df)

# COMMAND ----------



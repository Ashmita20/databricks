# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_result = dbutils.notebook.run("ingest_circuits_file",0,{"p_data_source":"Workflow"})

# COMMAND ----------

v_result

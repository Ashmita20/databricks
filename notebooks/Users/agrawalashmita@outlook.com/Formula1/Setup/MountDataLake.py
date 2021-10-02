# Databricks notebook source
storage_account_name = 'trialashmita20'
client_id = '6de55caa-5ca3-4f0e-bf8d-96e26d59c0bd'
tenant_id='4e700317-adae-4379-9226-883ce726d7a6'
secret_value='GvP7Q~5tigCpYMnCbQzyV_aTjgp4GsnVrhfrn'

# COMMAND ----------

covid_storage_account_name = 'covidreportingdl20'
client_id = '6de55caa-5ca3-4f0e-bf8d-96e26d59c0bd'
tenant_id='4e700317-adae-4379-9226-883ce726d7a6'
secret_value='GvP7Q~5tigCpYMnCbQzyV_aTjgp4GsnVrhfrn'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret":f"{secret_value}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %run "../include/Common_Functions"

# COMMAND ----------

mount_adls("raw",covid_storage_account_name)

# COMMAND ----------

mount_adls("processed",covid_storage_account_name)

# COMMAND ----------

mount_adls("lookup",covid_storage_account_name)

# COMMAND ----------

mount_adls("presentation",storage_account_name)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/trialashmita20/raw")

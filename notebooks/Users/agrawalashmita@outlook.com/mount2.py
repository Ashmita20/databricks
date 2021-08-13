# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs
# MAGIC #### Please update the following 
# MAGIC - application-id
# MAGIC - service-credential
# MAGIC - directory-id

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "14967248-8602-48f3-bd7e-253a599f139d",
           "fs.azure.account.oauth2.client.secret": "cGcxVkF2r8e5-45kwBe~6o524Odfz--NJK",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/4e700317-adae-4379-9226-883ce726d7a6/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://practise@customscript1234.dfs.core.windows.net/",
  mount_point = "/mnt/customscript1234/practise",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@customscript1234.dfs.core.windows.net/",
  mount_point = "/mnt/customscript1234/processed",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container
# MAGIC #### Update the storage account name before executing

# COMMAND ----------

dbutils.fs.ls("/mnt/customscript1234/practise")

# COMMAND ----------

file_location = "/mnt/customscript1234/practise/hospital_admissions.csv"
file_type = 'csv'
infer_Schema = 'true'
deliminter = ','
first_row_as_header= 'true'

df = spark.read.format(file_type) \
    .option("inferSchema",infer_Schema) \
    .option("sep",deliminter) \
    .option("header",first_row_as_header) \
    .load(file_location)
  

# COMMAND ----------

df.show()

# COMMAND ----------

val accesskey = dbutils.secrets.get(scope = "test", key = "test)

# COMMAND ----------

# Mount blob storage using Azure key vault
dbutils.fs.mount(
 source = "wasbs://practise@customscript1234.blob.core.windows.net",
 mount_point = "/mnt/customscript1234/practise",
 extra_configs = {"fs.azure.account.key.customscript1234.blob.core.windows.net":dbutils.secrets.get(scope = "test", key = "test")})

# COMMAND ----------

#Unmount blob storage
dbutils.fs.unmount("/mnt/customscript1234/practise")

# COMMAND ----------

file_location = "/mnt/customscript1234/practise/hospital_admissions.csv"
file_type = 'csv'
infer_Schema = 'true'
deliminter = ','
first_row_as_header= 'true'

df = spark.read.format(file_type) \
    .option("inferSchema",infer_Schema) \
    .option("sep",deliminter) \
    .option("header",first_row_as_header) \
    .load(file_location)

# COMMAND ----------

df2 = df.select("country","date","year_week")
display(df2)

# COMMAND ----------

# MAGIC %fs 
# MAGIC 
# MAGIC ls /mnt/customscript1234/practise

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls

# COMMAND ----------



# COMMAND ----------

target_folder_path = 'abfss://practise@customscript1234.dfs.core.windows.net/test/test.csv'
print(target_folder_path)      

# COMMAND ----------

df2.write.format("csv").mode("overwrite").option("inferSchema","true").option("header","true").save('/mnt/parctice/test/test.csv')
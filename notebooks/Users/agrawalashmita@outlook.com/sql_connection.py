# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "jdbc", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "jdbc", key = "password")

# COMMAND ----------

jdbcHostname = "practise-ash.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "practise-ash"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl ="jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# COMMAND ----------

prop = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  #"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
sql = "(select * from sys.tables) a"
df = spark \
.read\
.jdbc(url=jdbcUrl, table=sql, properties=prop)
display(df)

# COMMAND ----------

target_folder_path = '/dbfs/mnt/customscript1234/practise/sys_tables.csv'

# COMMAND ----------

df.toPandas().to_csv(target_folder_path,header =True,index=False)
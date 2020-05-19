# Databricks notebook source
# MAGIC %sh git clone https://github.com/lndkcg/HugeCTR.git

# COMMAND ----------

jdbcHostname = "lasr-sqldwdb-eastus2-prd.database.windows.net"
jdbcDatabase = "lasr-sqldwdb-prd"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

connectionProperties = {
  "user" : dbutils.secrets.get(scope = "nad_sqldw", key = "username"),
  "password" : dbutils.secrets.get(scope = "nad_sqldw", key = "password"),
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS FundSales
# MAGIC CREATE TEMPORARY VIEW FundSales AS
# MAGIC /dbfs/FileStore/TheData/
# MAGIC /Users/lndk@capgroup.com/Recommender/Data Prep
# Databricks notebook source
# general note: in the future, as more data is available in ADLS vs. SQL DW, we should look to move the sales alpha data prep steps to spark and ADLS as well
# that will help obviate the need to access SQL DW, and likely reduce the amount of code to pull this data

jdbcUser = 'lndk'
jdbcHostname = 'lasr-sqldwdb-eastus2-prd.database.windows.net'
jdbcDatabase = 'lasr-sqldwdb-prd'
jdbcPort = 1433

# COMMAND ----------

jdbcPassword = 'Syhur#26jh' #dbutils.secrets.get(scope = secret_scope, key = 'pw')

jdbcUrl = f'jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}'
connectionProperties = {
  'user' : jdbcUser,
  'password' : jdbcPassword,
  'driver' : 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

# COMMAND ----------

pushdown_query = f"""
(select Top 100 * from MSS_U_NAD_S.AlphaMaster) query
"""

print(pushdown_query)

# here's how this appraoch works
# 1. the cluster is now set to a minimun spark workder count of 0, and a maximum of 1
# 2. because we know have a spark workers, we can fire off spark commands, namely spark.read.jdbc
# 3. this seems much easier than other approaches
# 4. the drawback fo this is if the cluster has downscaled, which happens after a couple mins, you may wait 10ish mins for the spark worker to be re-provisioned
# 5. the job automatically runs after it's provisioned, so you don't need to do anything but wait
# 6. after running the query, it loads a spark data frame
# 7. which we then can convert to a pandas dataframe on the driver node, and single node python operations should be able to access it
# 8. the cluster will de-provision the worker node after a couple mins to save money
# when databricks goes GA with tf2 support sometime in CY Q1 2020, the provisioning time will drop to about 5 mins

spark_df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(spark_df)

# COMMAND ----------

import numpy as np
import pandas as pd

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
pandas_df = spark_df.select("*").toPandas()

# make sure to run an operation here in the same spot
# spark's lazy eval doesn't actually run the toPandas until you do something to force it, and the worker could idle and deprovision in that time from running the above spark read jdbc command
display(pandas_df)

# COMMAND ----------

display(pandas_df)

# COMMAND ----------


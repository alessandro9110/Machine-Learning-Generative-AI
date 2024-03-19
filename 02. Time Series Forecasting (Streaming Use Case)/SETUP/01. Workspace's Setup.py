# Databricks notebook source
# MAGIC %run ./00.Config

# COMMAND ----------

# MAGIC %md ### Create Catalog, schema and tables 

# COMMAND ----------


try:
    if CATALOG != 'hive_metastore':
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} ")
        spark.sql(f"USE CATALOG {CATALOG}")
    else:
        spark.sql("USE CATALOG hive_metastore")
except  Exception as ex:
    print(ex)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md ### Create a Volume

# COMMAND ----------

#spark.sql("CREATE VOLUME raw_streaming COMMENT 'This is a managed volume for the streaming files'")

# COMMAND ----------



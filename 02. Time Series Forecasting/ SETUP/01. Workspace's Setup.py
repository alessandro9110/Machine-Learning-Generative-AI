# Databricks notebook source
dbutils.widgets.text("Catalog", "hive_metastore")
dbutils.widgets.text("Schema", "streaming")

CATALOG = dbutils.widgets.get("Catalog")
SCHEMA  = dbutils.widgets.get("Schema")

# COMMAND ----------

# MAGIC %md ### Create a compute cluster

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Create a Job Cluster

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Create Catalog, schema and tables 

# COMMAND ----------

if CATALOG != 'hive_metastore':
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} ")
    spark.sql(f"USE CATALOG {CATALOG}")
else:
    spark.sql("USE CATALOG hive_metastore")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Create a Volume

# COMMAND ----------



# COMMAND ----------



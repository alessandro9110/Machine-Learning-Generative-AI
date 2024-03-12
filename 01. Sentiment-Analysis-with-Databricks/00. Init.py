# Databricks notebook source
dbutils.widgets.text(name="catalog_name", defaultValue="sentiment_analysis", label="catalog_name")
dbutils.widgets.text(name="schema_name",defaultValue="twitter", label="schema_name")

catalog_name  = dbutils.widgets.getArgument("catalog_name")
schema_name = dbutils.widgets.getArgument("schema_name")

# COMMAND ----------

# MAGIC %md ### Create a catalog and use it

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog_name};
# MAGIC
# MAGIC USE CATALOG  ${catalog_name};

# COMMAND ----------

# MAGIC %md ### Create Twitter schema

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS  ${schema_name};
# MAGIC

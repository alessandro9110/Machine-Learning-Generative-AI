# Databricks notebook source
CATALOG = 'streaming'
SCHEMA = 'streaming'

# COMMAND ----------

#dbutils.secrets.listScopes()

# COMMAND ----------

#app_client_id  = dbutils.secrets.get(scope="adb-kv-scope", key="app-client-id")
#secret_value    = dbutils.secrets.get(scope="adb-kv-scope", key="labadvanalytics-sp-secret")
#tenant_id       = dbutils.secrets.get(scope="adb-kv-scope", key="tenant-id")
#storage_account = dbutils.secrets.get(scope="adb-kv-scope", key="storage-account")

# COMMAND ----------

#spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
#spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", app_client_id)
#spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", secret_value)
#spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

#source = f"abfss://raw@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------



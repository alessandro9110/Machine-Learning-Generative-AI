# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/api/workspace/introduction

# COMMAND ----------

dbutils.widgets.text('pipeline_name', 'dlt-streaming-topic1')
pipeline_name = getArgument("pipeline_name")

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %run "../00.API CONFIG"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run DLT pipeline

# COMMAND ----------

# List all Pipelines
response = requests.get(
  'https://%s/api/2.0/pipelines' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  json={
}
)
# Filter pipeline with a specific pipeline name
for res in response.json()['statuses']:
  if res['name']==pipeline_name :
    update = requests.post(
      f"https://%s/api/2.0/pipelines/{res['pipeline_id']}/updates" % (DOMAIN),
      headers={'Authorization': 'Bearer %s' % TOKEN},
      json={'full_refresh': True
     }
     )
    print(update.json())

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

# MAGIC %md #### Stop DLT Pipeline

# COMMAND ----------

# Check if the DLT pipeline already exist
pipeline_exist = ''
get_pipelines = requests.get(
                            'https://%s/api/2.0/pipelines' % (DOMAIN),
                            headers={'Authorization': 'Bearer %s' % TOKEN},
                        )
if len(get_pipelines.json()) != 0:
    for i in get_pipelines.json()['statuses']:
        if i['name'] == pipeline_name:
            pipeline_exist = i
            print(f"The pipeline {pipeline_name}  exists")

# COMMAND ----------

# Create the pipeline if not exist
if pipeline_exist:
    # Stop Pipeline
    stop_pipeline = requests.post(
                                f"https://%s/api/2.0/pipelines/{pipeline_exist['pipeline_id']}/stop" % (DOMAIN),
                                headers={'Authorization': 'Bearer %s' % TOKEN},
                                json={})
    print(stop_pipeline.json())


# COMMAND ----------



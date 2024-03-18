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

# MAGIC %md #### Create DLT Pipeline

# COMMAND ----------

# List keys of a particular secret scope
response = requests.post(
  'https://%s/api/2.0/pipelines' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  json={
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 1
        }
    ],
    "development": True,
    "continuous": True,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/alessandro.armillotta@mitavanadeitaly.onmicrosoft.com/Machine-Learning-Generative-AI/02. Time Series Forecasting/01_Streaming_Pipeline/01.Delta Live Table Streaming"
            }
        }
    ],
    "name": pipeline_name,
    "edition": "CORE",
    "catalog": "streaming",
    "configuration": {
        "source_path": "abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/topic1"
    },
    "target": "streaming",
    "data_sampling": False
}
)
response.json()

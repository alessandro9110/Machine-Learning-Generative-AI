# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/api/workspace/introduction

# COMMAND ----------

dbutils.widgets.text('pipeline_name', 'dlt-streaming-ts')
pipeline_name = getArgument("pipeline_name")

dbutils.widgets.text('source_path1', 'abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/ts1')
source_path1 = getArgument("source_path1")

dbutils.widgets.text('source_path2', 'abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/ts2')
source_path2 = getArgument("source_path2")

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %run "../00.API CONFIG"

# COMMAND ----------

# MAGIC %md #### Create DLT Pipeline

# COMMAND ----------

# Check if the DLT pipeline already exist
pipeline_exist = False
get_pipelines = requests.get(
                            'https://%s/api/2.0/pipelines' % (DOMAIN),
                            headers={'Authorization': 'Bearer %s' % TOKEN},
                        )
if len(get_pipelines.json()) != 0:
    for i in get_pipelines.json()['statuses']:
        if i['name'] == pipeline_name:
            pipeline_exist = True
            print(f"The pipeline {pipeline_name} already exists")

# COMMAND ----------


# Create the pipeline if not exist
if not pipeline_exist:
    # Create Pipeline
    create_pipeline = requests.post(
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
        "photon": False,
        "libraries": [
            {
            "notebook": {
                "path": "/Repos/alessandro.armillotta@mitavanadeitaly.onmicrosoft.com/Machine-Learning-Generative-AI/02. Time Series Forecasting (Streaming Use Case)/01_Streaming_Pipeline/01.Delta Live Table Streaming TS1"
            }
        },
        {
            "notebook": {
                "path": "/Repos/alessandro.armillotta@mitavanadeitaly.onmicrosoft.com/Machine-Learning-Generative-AI/02. Time Series Forecasting (Streaming Use Case)/01_Streaming_Pipeline/01.Delta Live Table Streaming TS2"
            }
        }
        ],
        "name": pipeline_name,
        "edition": "CORE",
        "catalog": "streaming",
        "configuration": {
            "source_path1": source_path1,
            "source_path2": source_path2
        },
        "target": "streaming",
        "data_sampling": False
    }
    )
    print(create_pipeline.json())
    # Stop Pipeline
    stop_pipeline = requests.post(
                                f"https://%s/api/2.0/pipelines/{create_pipeline.json()['pipeline_id']}/stop" % (DOMAIN),
                                headers={'Authorization': 'Bearer %s' % TOKEN},
                                json={})
    print(stop_pipeline.json())


# COMMAND ----------



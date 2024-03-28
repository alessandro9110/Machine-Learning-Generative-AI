# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/api/workspace/introduction

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %run "../00.API CONFIG"

# COMMAND ----------

dbutils.widgets.text('job_name', 'Create Data Point for Time Series')
job_name = getArgument("job_name")

# COMMAND ----------

# MAGIC %md #### Create a job
# MAGIC Create a job with JSON configurations and Job name

# COMMAND ----------

# Check if JOB exist
job_exist = False
get_jobs = requests.get(
                            'https://%s/api/2.1/jobs/list' % (DOMAIN),
                            headers={'Authorization': 'Bearer %s' % TOKEN},
                        )
get_jobs = get_jobs.json()
del get_jobs['has_more']

if len(get_jobs) != 0:
    for i in get_jobs['jobs']:
        if i['settings']['name'] == job_name:
            job_exist = True
            print(f"The JOB {job_name} already exists")

# COMMAND ----------

if not job_exist:
  response = requests.post(
    'https://%s/api/2.1/jobs/create' % (DOMAIN),
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json={
      "name": job_name,
      "email_notifications": {
        "no_alert_for_skipped_runs": False
      },
      "webhook_notifications": {},
      "timeout_seconds": 0,
      "max_concurrent_runs": 1,
      "tasks": [
      {
        "task_key": "create_ts1_data_points",
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
          "notebook_path": "/Repos/alessandro.armillotta@mitavanadeitaly.onmicrosoft.com/Machine-Learning-Generative-AI/02. Time Series Forecasting (Streaming Use Case)/01_Streaming_Pipeline/00. Create Time Series",
          "base_parameters": {
            "storage_location": "abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/ts1"
          },
          "source": "WORKSPACE"
        },
        "job_cluster_key": "Job_cluster",
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": {
          "no_alert_for_skipped_runs": False,
          "no_alert_for_canceled_runs": False,
          "alert_on_last_attempt": False
        },
        "webhook_notifications": {}
      },
      {
        "task_key": "create_ts2_data_points",
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
          "notebook_path": "/Repos/alessandro.armillotta@mitavanadeitaly.onmicrosoft.com/Machine-Learning-Generative-AI/02. Time Series Forecasting (Streaming Use Case)/01_Streaming_Pipeline/00. Create Time Series",
          "base_parameters": {
            "storage_location": "abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/ts2"
          },
          "source": "WORKSPACE"
        },
        "job_cluster_key": "Job_cluster",
        "timeout_seconds": 0,
        "email_notifications": {}
      }
    ],
    "job_clusters": [
      {
        "job_cluster_key": "Job_cluster",
        "new_cluster": {
          "cluster_name": "",
          "spark_version": "13.3.x-scala2.12",
          "spark_conf": {
            "spark.master": "local[*, 4]",
            "spark.databricks.cluster.profile": "singleNode"
          },
          "azure_attributes": {
            "first_on_demand": 1,
            "availability": "ON_DEMAND_AZURE",
            "spot_bid_max_price": -1
          },
          "node_type_id": "Standard_DS3_v2",
          "custom_tags": {
            "ResourceClass": "SingleNode"
          },
          "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
          },
          "enable_elastic_disk": True,
          "data_security_mode": "SINGLE_USER",
          "runtime_engine": "STANDARD",
          "num_workers": 0
        }
      }
    ],
      "run_as": {
        "user_name": "alessandro.armillotta@mitavanadeitaly.onmicrosoft.com"
      }
    }
  )
  print(response.json())

# COMMAND ----------



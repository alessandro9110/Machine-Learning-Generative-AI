# Databricks notebook source
# MAGIC %md
# MAGIC https://docs.databricks.com/api/workspace/introduction

# COMMAND ----------

import requests

# COMMAND ----------

# MAGIC %run "./00.API CONFIG"

# COMMAND ----------

dbutils.widgets.text('job_name', 'Create Data Point for Time Series')
job_name = getArgument("job_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run the Job
# MAGIC Run the job based on the job id of before

# COMMAND ----------

response = requests.get(
  'https://%s/api/2.1/jobs/list' % (DOMAIN),
  headers={'Authorization': 'Bearer %s' % TOKEN},
  json={}
)

for res in response.json()['jobs']:
  if res['settings']['name']==job_name:
    update = requests.post(
      'https://%s/api/2.1/jobs/run-now' % (DOMAIN),
      headers={'Authorization': 'Bearer %s' % TOKEN},
      json={'job_id' : res['job_id']}
    )
    print(update.json())

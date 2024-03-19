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

# MAGIC %md
# MAGIC #### Stop all JOBS
# MAGIC Run the job based on the job id of before

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
            print(f"The JOB {job_name}  exists")
            run_job = requests.post(
                            'https://%s/api/2.1/jobs/runs/cancel-all' % (DOMAIN),
                            headers={'Authorization': 'Bearer %s' % TOKEN},
                            json={'job_id':i['job_id']}
                        )
            print(f"STOP JOB: {run_job.json()}")
            

# COMMAND ----------



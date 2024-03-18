# Databricks notebook source
# MAGIC %md
# MAGIC This notebooks is used to create a data point and store it into an External Location added previously to the Unity Catalog.
# MAGIC The notebook has a paraemter to create new topic and data folder into the storage account

# COMMAND ----------

dbutils.widgets.text("storage_location", "abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/topic1")
storage_location = getArgument("storage_location")

# COMMAND ----------

# MAGIC %run "../SETUP/00.Config"

# COMMAND ----------

dbutils.fs.rm(f"{storage_location}", True)

# COMMAND ----------

import pandas as pd
import datetime
import numpy as np
import time

# COMMAND ----------

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
tomorrow = today + datetime.timedelta(days=1)
yesterday = yesterday.strftime('%d/%m/%Y')
today = today.strftime('%d/%m/%Y')
tomorrow = tomorrow.strftime('%d/%m/%Y')
df = pd.date_range(start=today, end=tomorrow, freq="1min")
df = pd.DataFrame(df, columns= ['date'])
df['feature1'] = np.random.default_rng().choice(100)

# COMMAND ----------

for index, row in df.iterrows():
    #print(row)
    DF = row.to_frame().T
    DF['feature1'] = np.random.default_rng().choice(100)
    DF = spark.createDataFrame(DF)
    DF.write.format("json").mode('append').save(f"{storage_location}")
    #DF.display()
    time.sleep(3)

# COMMAND ----------



# COMMAND ----------



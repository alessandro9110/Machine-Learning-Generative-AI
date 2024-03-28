# Databricks notebook source
# MAGIC %md
# MAGIC This notebooks is used to create a data point and store it into an External Location added previously to the Unity Catalog.
# MAGIC The notebook has a paraemter to create new topic and data folder into the storage account

# COMMAND ----------

dbutils.widgets.text("storage_location", "abfss://raw@labadvanalytics.dfs.core.windows.net/IoT_Data/ts1")
storage_location = getArgument("storage_location")

# COMMAND ----------

# MAGIC %run "../SETUP/00.Config"

# COMMAND ----------

#dbutils.fs.rm(f"{storage_location}", True)

# COMMAND ----------

import pandas as pd
import datetime
import numpy as np
import time
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md #### Create Synthetic Data
# MAGIC

# COMMAND ----------

# Predefined paramters
ar_n = 3                     # Order of the AR(n) data
ar_coeff = [0.7, -0.3, -0.1] # Coefficients b_3, b_2, b_1
noise_level = 0.1            # Noise added to the AR(n) data
length = 43800                 # Number of data points to generate
 
# Random initial values
ar_data = list(np.random.randn(ar_n))
 
# Generate the rest of the values
for i in range(length - ar_n):
    next_val = (np.array(ar_coeff) @ np.array(ar_data[-3:])) + np.random.randn() * noise_level
    ar_data.append(next_val)

# COMMAND ----------

df =pd.DataFrame({'feature1': ar_data})

# COMMAND ----------

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
tomorrow = today + datetime.timedelta(days=1)
yesterday = yesterday.strftime('%d/%m/%Y')
today = today.strftime('%d/%m/%Y')
tomorrow = tomorrow.strftime('%d/%m/%Y')
df['date'] = pd.date_range(start=today, periods=len(ar_data), freq="1min")

# COMMAND ----------

df

# COMMAND ----------

for index, row in df.iterrows():
    #print(row)
    DF = row.to_frame().T
    DF['feature1'] = np.random.default_rng().choice(100)
    DF = spark.createDataFrame(DF)
    DF.write.format("json").mode('append').save(f"{storage_location}")
    #DF.display()
    time.sleep(10)

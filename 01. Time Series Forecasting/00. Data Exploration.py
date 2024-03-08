# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

df = spark.read.format('csv').option('delimiter',';').option('header', True).load('abfss://raw@armillottastorage.dfs.core.windows.net/time_series/household_power_consumption.txt')

df = df.select('Date', 'Time', 'Global_active_power', 'Global_reactive_power')

# COMMAND ----------

# MAGIC %md ### Describe Data

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df.summary().display()

# COMMAND ----------

# MAGIC %md ### Missing Values

# COMMAND ----------

# MAGIC %md #### Check zeros

# COMMAND ----------

df.filter(F.col('Global_active_power')==0).display()

# COMMAND ----------

df.groupBy('Date').agg(F.count(F.col('Date')).alias('count')).filter(F.col('count')<1440).display()

# COMMAND ----------

# MAGIC %md #### Drop 16/12/2006
# MAGIC
# MAGIC DRop this time series because it one day and it's the starting one

# COMMAND ----------

df.filter(F.col('Date')=='16/12/2006').display()

# COMMAND ----------

df = df.filter(F.col('Date')!='16/12/2006')

# COMMAND ----------

# MAGIC %md #### 26/11/2010

# COMMAND ----------

df.filter(F.col('Date')=='26/11/2010').display()

# COMMAND ----------

# MAGIC %md ### Create new columns

# COMMAND ----------

df =(df.withColumn('timestamp', F.concat('Date', F.lit(' '), 'Time')))

# COMMAND ----------

DF = df.withColumn('timestamp', F.to_timestamp(F.col('timestamp'), 'd/M/y H:mm:ss'))

# COMMAND ----------



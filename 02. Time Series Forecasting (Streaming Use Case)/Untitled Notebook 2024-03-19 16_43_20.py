# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.pandas as ps
from pyspark.sql.types import *
import pandas as pd

# COMMAND ----------

df = spark.read.table("streaming.streaming.bronze_ts1")

# COMMAND ----------

df.display()

# COMMAND ----------

# Primo step eliminare i duplicati prendendo i dati dell'ultimo files in input
wf = Window.partitionBy('date').orderBy(F.col('processing_time').desc())
df = df.withColumn("rn",F.row_number().over(wf)).filter(F.col('rn')==1).drop('rn')

# Fillare i timestamp mancanti con la media orario per ogni weekday
df=df.withColumn('date', F.col('date').astype(TimestampType()) )

date = df.select(F.max('date').alias('max_date'), F.min('date').alias('min_date')).toPandas()
date = ps.DataFrame({'date':ps.date_range(start=date['min_date'][0], end=date['max_date'][0],freq="1min").to_numpy()}).to_spark()
date = date.withColumn('date', F.col('date').astype(TimestampType()) )
#date.display()

date.join(df, how='left', on='date').display()


# COMMAND ----------

date_all.display()

# COMMAND ----------



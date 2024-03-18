# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
    name = "topic",
    comment = "Raw data IoT device",

)
def topic():
    source = spark.conf.get("source_path")
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}")
        .select(
            current_timestamp().alias("processing_time"),
            #input_file_name().alias("source_file"),
            "*"
        )
    )

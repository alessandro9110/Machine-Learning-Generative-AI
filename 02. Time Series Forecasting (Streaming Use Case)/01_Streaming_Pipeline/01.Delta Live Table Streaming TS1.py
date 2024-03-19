# Databricks notebook source
import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source_path")

# COMMAND ----------

@dlt.table(
    name = "bronze_TS1",
    comment = "Raw data IoT device",
    table_properties = {"quality": "bronze"}

)
def bronze_ts1():
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}")
        .select(
            F.current_timestamp().alias("processing_time"),
            #F.input_file_name().alias("source_file"),
            "*"
        )
    )

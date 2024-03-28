# Databricks notebook source
import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source_path2")

# COMMAND ----------

@dlt.table(
    name = "bronze_TS2",
    comment = "Raw data IoT device",
    table_properties = {"quality": "bronze"}

)
def bronze_ts2():
    
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

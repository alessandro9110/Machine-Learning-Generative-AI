# Databricks notebook source
#%pip install sacremoses==0.0.53

# COMMAND ----------

from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output
from transformers import pipeline
from datasets import load_dataset, concatenate_datasets
load_dataset.utils.logging.disable_progress_bar()

import pandas as pd
import os
import pyspark.sql.functions as F

import mlflow
from mlflow.tracking import MlflowClient

# COMMAND ----------

arctifact_location = "dbfs:/FileStore/mlflow"


# COMMAND ----------

# MAGIC %md ### Sentiment Analysis

# COMMAND ----------

# DBTITLE 1,Set Experiment
experiment_name = "/Shared/SentimentAnalysis"

try:
  experiment_id = mlflow.create_experiment(name = experiment_name
                                          ,artifact_location = arctifact_location
                                          ,tags={'exp_name': experiment_name})
  
  experiment = mlflow.get_experiment_by_name(experiment_name)
  experiment = mlflow.set_experiment(experiment_id = experiment.experiment_id)
  
except:
  print(f"Experiment {experiment_name} already exist.")
  experiment = mlflow.get_experiment_by_name(experiment_name)
  experiment = mlflow.set_experiment(experiment_id = experiment.experiment_id)

# COMMAND ----------

# DBTITLE 1,Load HF dataset and save it as Delta Table
list_values = {'0': 'negative', '2' : 'neutral', '4' : 'positive'}
dataset = load_dataset("sentiment140")
dataset = concatenate_datasets([dataset['train'], dataset['test']])
dataset = pd.DataFrame(dataset)

dataset = spark.createDataFrame(dataset).select('text','sentiment')

dataset = dataset.replace(list_values,subset=['sentiment'])
dataset.write.mode('overwrite').saveAsTable('bronze_sentiment')
dataset = spark.read.table('bronze_sentiment').limit(100)
dataset.display()

# COMMAND ----------

# MAGIC %sql CREATE SCHEMA  IF NOT EXISTS  test;

# COMMAND ----------

model = pipeline(task = "text-classification", 
                      model="FacebookAI/roberta-large-mnli")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf("string")
def apply_model(s: pd.Series) -> pd.Series:
    s = s.to_list()
    result = model(s)
    result = [r['label'] for r in result]
    return pd.Series(result)

df = dataset.select('text', 'sentiment',  apply_model(F.col("text")).alias('sentiment_prediction'))

#df.write.mode('overwrite').saveAsTable('test.bronze_sentiment_with_label_predicted')
df.display()

# COMMAND ----------

# with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=t5-small) as run:
with mlflow.start_run(experiment_id=experiment.experiment_id, run_name="bert-model") as run:

    model = pipeline(task="text-classification",
                      model="nickwong64/bert-base-uncased-poems-sentiment")
    
    



    mlflow.end_run()

# COMMAND ----------

# with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=t5-small) as run:
with mlflow.start_run(experiment_id=experiment.experiment_id, run_name="roberta-model") as run:


    model = pipeline(task = "text-classification", 
                      model="FacebookAI/roberta-large-mnli")




    mlflow.end_run()

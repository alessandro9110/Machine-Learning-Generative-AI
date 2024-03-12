# Databricks notebook source
#%pip install sacremoses==0.0.53

# COMMAND ----------

from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output
from transformers import pipeline
from datasets import load_dataset, concatenate_datasets

import pandas as pd
import pyspark.sql.functions as F

import mlflow
from mlflow.tracking import MlflowClient

# COMMAND ----------

arctifact_location = "dbfs:/FileStore/mlflow"


# COMMAND ----------

# MAGIC %md ### Sentiment Analysis

# COMMAND ----------

# DBTITLE 1,Set Experiment
experiment_name = "/Shared/MLFlow Experiments/SentimentAnalysis"

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
#dataset = load_dataset("sentiment140")
#dataset = concatenate_datasets([dataset['train'],dataset['test']])
#dataset = pd.DataFrame(dataset)
#dataset = spark.createDataFrame(dataset)
#dataset.write.mode('overwrite').saveAsTable('test.sentiment_analysis.bronze_sentiment')
dataset = spark.read.table('test.sentiment_analysis.bronze_sentiment')
dataset.display()

# COMMAND ----------

model = pipeline(task="text-classification",
                      model="nickwong64/bert-base-uncased-poems-sentiment")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

@pandas_udf("string")
def apply_model(s: pd.Series) -> pd.Series:
    s = s.to_list()
    result = model(s)
    result = [r['label'] for r in result]
    return pd.Series(result)

df = dataset.withColumn('result', apply_model(F.col("text")))
df.write.mode('overwrite').saveAsTable('test.sentiment_analysis.bronze_sentiment_with_label_predicted')

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

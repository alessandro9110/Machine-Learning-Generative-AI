# Databricks notebook source
from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output
from transformers import pipeline

import mlflow
from mlflow import MlflowClient

# COMMAND ----------

# MAGIC %md ###
# MAGIC Load an HuggingFace model and register it into MLFlow

# COMMAND ----------

model = pipeline(task = "text-classification", 
                      model="FacebookAI/roberta-large-mnli")

print(model(["Today I'm sad"]))

mlflow.transformers.log_model(model,artifact_path='roberta_large_mnli', registered_model_name='roberta-large-mnli')

# COMMAND ----------

client = MlflowClient()
model = client.get_registered_model('roberta-large-mnli')

model = mlflow.pyfunc.load_model(dict(dict(model)['latest_versions'][0])['source'])
model.predict('prova')

# COMMAND ----------



# COMMAND ----------



# Databricks notebook source
import requests
import pandas as pd

# COMMAND ----------


path = 'https://viacep.com.br/ws/05425070/json/'
response = requests.get(path)
json_result = response.json()
df_cep = spark.createDataFrame([json_result])
df_cep.write.saveAsTable("dev.cep_bronze")

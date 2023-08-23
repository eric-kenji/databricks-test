# Databricks notebook source
import requests
import dlt
path = 'https://viacep.com.br/ws/05425070/json/'
response = requests.get(path)
json_result = response.json()

@dlt.create_table(
  comment="The raw cep data, ingested from api."
)
def dlt_cep_bronze():
  return (
    spark.createDataFrame([json_result])
  )


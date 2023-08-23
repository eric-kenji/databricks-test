# Databricks notebook source
import requests
import dlt

@dlt.create_table(
  comment="The silver cep data, containing specific and renamed columns."
)
def dlt_cep_bronze():
  return (
    spark.read.table("LIVE.cep_bronze")\
    .select("bairro","cep","complemento","localidade","logradouro","uf")\
    .withColumnRenamed("logradouro","rua")
  )


# Databricks notebook source
# Select specific columns and rename it

var_environment = dbutils.widgets.get("var_environment")

df_cep_silver = spark.read.table(f"{var_environment}.cep_bronze")\
    .select("bairro","cep","complemento","localidade","logradouro","uf")\
    .withColumnRenamed("logradouro","rua")

df_cep_silver.write.saveAsTable(f"{var_environment}.cep_silver")

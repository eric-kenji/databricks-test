# Databricks notebook source
# Select specific columns and rename it

df_cep_silver = spark.read.table("dev.cep_bronze")\
    .select("bairro","cep","complemento","localidade","logradouro","uf")\
    .withColumnRenamed("logradouro","rua")

df_cep_silver.write.saveAsTable("dev.cep_silver")

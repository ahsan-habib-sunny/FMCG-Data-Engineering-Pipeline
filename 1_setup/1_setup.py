# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC -- Catalog creation
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg ;
# MAGIC USE CATALOG fmcg ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Creating the schema
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze ;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver ;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold ;

# COMMAND ----------


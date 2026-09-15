# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog helios

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists bronze
# MAGIC managed location "abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/bronze/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver
# MAGIC managed location "abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/silver/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold
# MAGIC managed location "abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/gold/"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists landing
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  EXTERNAL  VOLUME  IF NOT EXISTS  landing
# MAGIC     LOCATION 'abfss://helios-raw-storage@projectstorage12.dfs.core.windows.net/helios_medallion/landing'
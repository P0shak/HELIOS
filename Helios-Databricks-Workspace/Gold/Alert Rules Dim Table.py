# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS helios.gold.dim_alert_info (
# MAGIC     alert_id INT,
# MAGIC     alert_name STRING,
# MAGIC     message STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO helios.gold.dim_alert_info (alert_id, alert_name, message)
# MAGIC VALUES
# MAGIC     (1, 'Low', 'Low level detected.'),
# MAGIC     (2, 'Moderate', 'Monitor the asset'),
# MAGIC     (3, 'Warning', 'Attention required.'),
# MAGIC     (4, 'Critical', 'Needs clinical inspection');
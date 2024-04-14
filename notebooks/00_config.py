# Databricks notebook source
if "config" not in locals():
    config = {}

# COMMAND ----------

# DBTITLE 1,Source URL
config['aqi_data_url'] = "https://www.aqi.in/dashboard/india/delhi/new-delhi"

# COMMAND ----------

# DBTITLE 1,Source ADLS location
config['adls_location'] = "/mnt/aqi_source/"

# COMMAND ----------

# DBTITLE 1,Base location
config['base_location'] = "dbfs:/tmp/new_delhi_aqi_analysis/"

# COMMAND ----------

# DBTITLE 1,Database Name
config["database_name"] = 'new_delhi_aqi'

# COMMAND ----------

# DBTITLE 1,Table Checkpoint Location
# config["checkpoint_loc"] = config['base_location'] + "table_checkpoints/"

# COMMAND ----------

# DBTITLE 1,Creating Database
_ = spark.sql("create database if not exists {0} location '/tmp/new_delhi_aqi_analysis'".format(config["database_name"]))
# setting the current database
_ = spark.catalog.setCurrentDatabase(config['database_name'])
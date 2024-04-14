# Databricks notebook source
# DBTITLE 1,Initializing Configuration
# MAGIC %run "./00_config"

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import json
import requests
import ast
import datetime

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Getting Data from Website
res = str(requests.get(config['aqi_data_url']).content)

# COMMAND ----------

# MAGIC %md
# MAGIC As data from the website contains a lot of data, but we just need data about the city and it's location, and for the ease of slicing the data we converted it in string format, and now we are going to perform slicing on the data so that we will only have required data.

# COMMAND ----------

# DBTITLE 1,String Slicing
res = res[res.find("var singlelocations = ")+22:]

# COMMAND ----------

raw_data = res[:res.find(";")]

# COMMAND ----------

# MAGIC %md
# MAGIC Above operations are website specific, they are not part of any standard process for web scrapping.

# COMMAND ----------

# our raw data contains "\\n" we need to remove that or else it will be problematic while saving data
raw_data = raw_data.replace("\\n", "")

# COMMAND ----------

raw_data

# COMMAND ----------

# MAGIC %md
# MAGIC After making our data workable we are going to save it in form of json document in dbfs, more specifically mounted ADLS.

# COMMAND ----------

# creating a dictionary
dictionary = ast.literal_eval(raw_data)

# COMMAND ----------

# DBTITLE 1,Mounting ADLS if not mounted
# MAGIC %run "./_mounting_ADLS"

# COMMAND ----------

current_time = f"{datetime.datetime.now()}"

# COMMAND ----------

# DBTITLE 1,Saving data as JSON file in ADLS
file_name = f"/dbfs/{config['adls_location']}" + (f"{current_time}"+"_"+".json").replace(" ", "")
# save data in json file
with open(f"/dbfs/{config['adls_location']}" + (f"{current_time}"+"_"+".json").replace(" ", ""), "w") as file:
    json.dump(dictionary, file)

# COMMAND ----------

# DBTITLE 1,Saving Metadata
# reading meta data
with open(f"/dbfs/{config['adls_location']}/_metadata.json") as file:
    dic = json.load(file)

# updating data for metadata
dic[f"{len(dic) + 1}"] = {"action_type":"w",
 "time_stamp":f"{current_time}",
 "file_name":f"{file_name.split('/')[-1]}",
 "is_read": False}

# writing to metadata
with open(f"/dbfs/{config['adls_location']}/_metadata.json", "w") as file:
    json.dump(dic, file)
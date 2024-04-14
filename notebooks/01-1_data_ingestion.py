# Databricks notebook source
# MAGIC %md After loading data in JSON format in adls, we now need to read it, and store it in Databricks as delta table so that it can be used for downstream applications.

# COMMAND ----------

# DBTITLE 1,Initializing Configuration
# MAGIC %run "./00_config"

# COMMAND ----------

# DBTITLE 1,Mounting ADLS if not mounted
# MAGIC %run "./_mounting_ADLS"

# COMMAND ----------

# DBTITLE 1,Importing Libraries
from pyspark.sql.functions import *
import json
import time

# COMMAND ----------

def create_delta_table_aqi(filename):
        # loading raw data
        load_data = (spark.read.format("json").load(filename))
        # reading state data
        df_state = spark.createDataFrame(load_data.select("stateData").collect()[0][0])
        # reading location data
        df_loc = spark.createDataFrame(load_data.select("Locations").collect()[0][0])
        # transforming and writing data to delta table
        _ = (df_state
            .drop('airComponents')
            .join(
            df_state
                .withColumn('compAir', explode(col('airComponents'))) \
                .drop('airComponents') \
                .selectExpr("locationName","compAir.*") \
                .drop("sensorUnit") \
                .groupBy('locationName') \
                .pivot("sensorName").sum("sensorData"),
                on="locationName")   
                .drop('formatdate', 'timeago')) \
                .select('locationName',
                    'cityName',
                    'countryName',
                    'locationId',
                    'searchType',
                    'source',
                    'sourceUrl',
                    'stateName',
                    'AQI-IN',
                    'aqi',
                    'co',
                    'dew',
                    'h',
                    'no2',
                    'o3',
                    'p',
                    'pm10',
                    'pm25',
                    'so2',
                    't',
                    'w',
                    'wd',
                    'wg') \
            .union(
            (df_loc
            .drop('airComponents')
            .join(
            df_loc
                .withColumn('compAir', explode(col('airComponents'))) \
                .drop('airComponents') \
                .selectExpr("locationName","compAir.*") \
                .drop("sensorUnit") \
                .groupBy('locationName') \
                .pivot("sensorName").sum("sensorData"),
                on="locationName"        
            )
            .drop('lat', 'lon', 'sensorcount', 'updated_at', 'timestamp', 'timeago')
            .select('locationName',
                'cityName',
                'countryName',
                'locationId',
                'searchType',
                'source',
                'sourceUrl',
                'stateName',
                'AQI-IN',
                'aqi',
                'co',
                'dew',
                'h',
                'no2',
                'o3',
                'p',
                'pm10',
                'pm25',
                'so2',
                't',
                'w',
                'wd',
                'wg'))
        ).withColumn("timestamp", current_timestamp()).write.mode("append").saveAsTable("raw_aqi_data")

# COMMAND ----------

# reading meta data
with open(f"/dbfs/{config['adls_location']}/_metadata.json") as file:
    dic = json.load(file)

# extracting filename
for _ in range(1, len(dic)+1):
    # print(_)
    print(dic[f"{_}"])
    if dic[f"{_}"]["is_read"] == False:
        create_delta_table_aqi(filename=f"{config['adls_location']}{dic[f'{_}']['file_name']}")
        # setting 'is_read' to True
        dic[f"{_}"]["is_read"] = True

# writing to metadata
with open(f"/dbfs/{config['adls_location']}/_metadata.json", "w") as file:
    json.dump(dic, file)
# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

arr_1 = [1, 2, 3, 4, 5]
arr_2 = [1, 2, 3]

# COMMAND ----------

def get_unsimilar_values(arr_1 : list, arr_2 : list):
    pass

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.new_delhi_aqi.aqi_record;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.new_delhi_aqi.current_aqi_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.new_delhi_aqi.raw_aqi_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select `timestamp`, locationName from delhi_aqi.aqi_data
# MAGIC where locationName = "Alipur"
# MAGIC order by `timestamp` desc, locationName;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from delhi_aqi.aqi_data;

# COMMAND ----------

127 * 3

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table delhi_aqi.aqi_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT s1.locationName as Location,
# MAGIC   coalesce(s1.aqi, 0) as `AQI_US`,
# MAGIC   coalesce(s1.pm25, 0) as `PM2_5`,
# MAGIC   coalesce(s1.pm10, 0) as PM10,
# MAGIC   coalesce(s1.h, 0) as Humidity,
# MAGIC   coalesce(s1.t, 0) as Temperature,
# MAGIC   CASE
# MAGIC   WHEN `AQI_US` BETWEEN 0 AND 50 THEN 'Good'
# MAGIC   WHEN `AQI_US` BETWEEN 51 AND 100 THEN 'Moderate'
# MAGIC   WHEN `AQI_US` BETWEEN 101 AND 150 THEN 'Unhealthy'
# MAGIC   WHEN `AQI_US` BETWEEN 151 AND 200 THEN 'Unhealthy for Strong People	'
# MAGIC   WHEN `AQI_US` > 200 THEN 'Hazardous'
# MAGIC   END AS AQI_Level
# MAGIC   FROM delhi_aqi.aqi_data s1
# MAGIC   INNER JOIN
# MAGIC   (
# MAGIC     SELECT locationName, max(`timestamp`) AS mts
# MAGIC     FROM delhi_aqi.aqi_data 
# MAGIC     GROUP BY locationName 
# MAGIC   ) s2 ON s2.locationName = s1.locationName AND s1.`timestamp` = s2.mts

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delhi_aqi.current_aqi_data;

# COMMAND ----------

display(spark.sql("select locationName, coalesce(aqi, 0), coalesce(co, 0)  from delhi_aqi.current_aqi_data where locationName='IIT ISM';"))

# COMMAND ----------

df = spark.sql("select * from delhi_aqi.aqi_data;")
display(df)
# SELECT ISNULL(myColumn, 0 ) FROM myTable

# COMMAND ----------

display(
    spark.sql("""              
              (  SELECT locationName,
                avg(`AQI-IN`) as av_aqiin,
                avg(aqi),
                avg(co),
                avg(dew),
                avg(h),
                avg(no2),
                avg(o3),
                avg(p),
                avg(pm1),
                avg(pm10),
                avg(pm25),
                avg(r),
                avg(so2),
                avg(t),
                avg(w),
                avg(wd),
                avg(wg)
                FROM delhi_aqi.aqi_data
                GROUP BY locationName)
              """)
)

# COMMAND ----------

for _ in df.columns:
    print(f"{_} :"+ f"{df.where(F.isnull(F.col(f'{_}'))).count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table aqi_data;
# MAGIC -- create table raw_aqi_data
# MAGIC -- as 
# MAGIC -- (
# MAGIC --   SELECT * FROM delta.`dbfs:/user/hive/warehouse/delhi_aqi.db/aqi_data/`
# MAGIC -- );

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended temp_raw_aqi_data

# COMMAND ----------

for i in range(len(dbutils.fs.ls("/mnt/aqi_source/"))):
    print(dbutils.fs.ls("/mnt/aqi_source/")[1][0].split("/")[-1])
    if 

# COMMAND ----------

from datetime import datetime
current_time = datetime.now()

# COMMAND ----------

dic_write = {
    0 : {
        "write_timestamp" : current_time,
        "write_file_name" : "" 
         }
}

# COMMAND ----------

dic_read = {
    0 : {
        "read_timestamp" : current_time,
        "read_file_name" : "<file_name>"
    }
}
(dic_read)

# COMMAND ----------

def load_data(table_name : str,
              make_checkpoint : bool,
              base_location : str):
    pass

# COMMAND ----------

check_point = {
    0 : {
        "read_time" : "",
        "read_file_name" : "",
        "read_file_location" : "",
        "file_write_time" : ""
    }
}
print(check_point)

# COMMAND ----------

if dic['read_timestamp'] > (datetime.fromtimestamp(1689241900000/1000)):
    print("later")

# COMMAND ----------

str(datetime.fromtimestamp(dbutils.fs.ls("/mnt/aqi_source/")[0][3]/1000))
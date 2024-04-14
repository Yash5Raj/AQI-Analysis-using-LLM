# Databricks notebook source
# DBTITLE 1,Initializing Configuration
# MAGIC %run "./00_config"

# COMMAND ----------

# DBTITLE 1,Creating View for Current Data
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW current_aqi_data
# MAGIC AS
# MAGIC (
# MAGIC   SELECT s1.locationName as Location,
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
# MAGIC   FROM new_delhi_aqi.raw_aqi_data s1
# MAGIC   INNER JOIN
# MAGIC   (
# MAGIC     SELECT locationName, max(`timestamp`) AS mts
# MAGIC     FROM new_delhi_aqi.raw_aqi_data 
# MAGIC     GROUP BY locationName 
# MAGIC   ) s2 ON s2.locationName = s1.locationName AND s1.`timestamp` = s2.mts
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Creating view for historical data
# MAGIC %sql
# MAGIC create or replace view aqi_record
# MAGIC as
# MAGIC (
# MAGIC   select
# MAGIC   -- DATE(timestamp) AS date,
# MAGIC   distinct cast(`timestamp` as date) as Date,
# MAGIC   locationName as Location,
# MAGIC   coalesce(h, 0) as Humidity,
# MAGIC   coalesce(t, 0) as Temperature,
# MAGIC   coalesce(aqi, 0) as AQI_US,
# MAGIC   CASE
# MAGIC   WHEN `AQI_US` BETWEEN 0 AND 50 THEN 'Good'
# MAGIC   WHEN `AQI_US` BETWEEN 51 AND 100 THEN 'Moderate'
# MAGIC   WHEN `AQI_US` BETWEEN 101 AND 150 THEN 'Unhealthy'
# MAGIC   WHEN `AQI_US` BETWEEN 151 AND 200 THEN 'Unhealthy for Strong People	'
# MAGIC   WHEN `AQI_US` > 200 THEN 'Hazardous'
# MAGIC   END AS Air_Quality,
# MAGIC   coalesce(pm25, 0) as PM2_5_Level,
# MAGIC   coalesce(pm10, 0) as PM10_Level,
# MAGIC   coalesce(co, 0) as CO_Level,
# MAGIC   coalesce(no2, 0) as NO2_Level,
# MAGIC   coalesce(o3, 0) as O3_Level,
# MAGIC   coalesce(so2, 0) as SO2_Level
# MAGIC from new_delhi_aqi.raw_aqi_data
# MAGIC order by Date desc
# MAGIC );
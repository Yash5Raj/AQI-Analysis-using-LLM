# Databricks notebook source
try:
  # connecting to ADLS container using Access Key
  dbutils.fs.mount(
  source = "wasbs://aqi-raw@strgyr.blob.core.windows.net",
  mount_point = "/mnt/aqi_source",
  extra_configs = {"fs.azure.account.key.strgyr.blob.core.windows.net":"0xHQkkW1eReUhlv8wsRqSlmVQTTF8yTC4MuG7u56EYs5pGj2U+5KaLeHOKifWyZqoAG4m+DJxR5f+AStM/Vr0w=="})
except:
  print("Already Mounted!!")
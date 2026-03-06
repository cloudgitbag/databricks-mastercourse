# Databricks notebook source
#%fs rm /Volumes/masterdb_dev/demo_db/files/dataset_ch10/babynames.csv

# COMMAND ----------

dbutils.fs.cp("/Volumes/masterdb_dev/demo_db/files/babynames.csv", "/Volumes/masterdb_dev/demo_db/files/dataset_ch10/")

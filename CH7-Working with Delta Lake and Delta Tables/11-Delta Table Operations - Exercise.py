# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cleanup previous runs

# COMMAND ----------

# MAGIC %run ../utils/cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC #####Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE CATALOG IF NOT EXISTS dev;
# MAGIC -- CREATE DATABASE IF NOT EXISTS dev.demo_db;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE masterdb_dev.demo_db.people(
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   lastName STRING,
# MAGIC   birthDate STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT OVERWRITE TABLE masterdb_dev.demo_db.people
# MAGIC SELECT id, fname as firstName, lname as lastName, dob as birthDate
# MAGIC FROM JSON.`/Volumes/masterdb_dev/demo_db/files/people.json`;
# MAGIC
# MAGIC SELECT * FROM masterdb_dev.demo_db.people;

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Delete one record from the above table using Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE from masterdb_dev.demo_db.people where firstName='M David';
# MAGIC
# MAGIC SELECT * FROM  masterdb_dev.demo_db.people

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY masterdb_dev.demo_db.people

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Delete one record from the above table using API

# COMMAND ----------

from delta import DeltaTable 

people_dt = DeltaTable.forName(spark,"masterdb_dev.demo_db.people")

people_dt.delete("firstName='abdul'")
people_dt.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Update one record in the delta table using API

# COMMAND ----------

import pyspark.sql.functions as F 
people_dt.update(
    condition="birthDate='1975-05-25'",
    set={"firstName": F.initcap("firstName"), "lastName": F.initcap("lastName")}
)

display(people_dt.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC #####4. Merge the given dataframe into the delta table

# COMMAND ----------

source_df = spark.read.format("json").load("/Volumes/masterdb_dev/demo_db/files/people.json")
display(source_df)

# COMMAND ----------

(
    people_dt.alias("t")
    .merge(source_df.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

people_dt.toDF().display()

# COMMAND ----------

(
    people_dt.alias("t")
    .merge(source_df.alias("s"), "t.id = s.id")
    .whenMatchedDelete(condition="t.firstName='Kailash' and t.lastName='Patil'")
    .whenMatchedUpdate(condition="t.id= 101",set={"t.birthDate":"'1975-06-25'"})
    .whenMatchedUpdate(set={"t.id":"s.id","t.firstName":"s.fname","t.lastName":"s.lname","t.birthDate":"s.dob"})
    #.whenMatchedUpdateAll()
    .whenNotMatchedInsert(values={"t.id":"s.id","t.firstName":"s.fname","t.lastName":"s.lname","t.birthDate":"s.dob"})
    #.whenNotMatchedInsertAll()
    .execute()
)

people_dt.toDF().display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
# MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>

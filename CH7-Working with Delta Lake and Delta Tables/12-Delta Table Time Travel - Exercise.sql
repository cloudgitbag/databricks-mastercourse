-- Databricks notebook source
-- MAGIC %md
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://blog.scholarnest.com/wp-content/uploads/2023/03/scholarnest-academy-scaled.jpg" alt="ScholarNest Academy" style="width: 1400px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Cleanup previous runs

-- COMMAND ----------

-- MAGIC %run ../utils/cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Setup

-- COMMAND ----------

-- CREATE CATALOG IF NOT EXISTS dev;
-- CREATE DATABASE IF NOT EXISTS dev.demo_db;
DROP TABLE IF EXISTS masterdb_dev.demo_db.people;

CREATE OR REPLACE TABLE masterdb_dev.demo_db.people(
  id INT,
  firstName STRING,
  lastName STRING,
  birthDate STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Load data into the delta table

-- COMMAND ----------

INSERT OVERWRITE TABLE masterdb_dev.demo_db.people
SELECT id, fname as firstName, lname as lastName, dob as birthDate
FROM JSON.`/Volumes/masterdb_dev/demo_db/files/people.json`;

SELECT * FROM masterdb_dev.demo_db.people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Delete one record from the delta table

-- COMMAND ----------

DELETE from masterdb_dev.demo_db.people where firstName='M David';

SELECT * FROM  masterdb_dev.demo_db.people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Update one record in delta table

-- COMMAND ----------

UPDATE masterdb_dev.demo_db.people
Set firstName =initcap(firstName),
    lastName = initcap(lastName)
Where birthDate ='1975-05-25';

SELECT * FROM  masterdb_dev.demo_db.people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Execute a merge statement into a delta table

-- COMMAND ----------

merge into masterdb_dev.demo_db.people tgt
using (select id, fname as firstName, lname as lastName, dob as birthDate
       from json.`/Volumes/masterdb_dev/demo_db/files/people.json`) src
on tgt.id = src.id
when matched and tgt.firstName = 'Kailash' then
  delete
when matched then
  update set tgt.birthDate = src.birthDate
when not matched then
  insert *;

SELECT * FROM  masterdb_dev.demo_db.people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Show delta table version history

-- COMMAND ----------

DESCRIBE HISTORY masterdb_dev.demo_db.people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Show the most recent version of the data from delta table

-- COMMAND ----------

SELECT * FROM masterdb_dev.demo_db.people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Show version 1 of the data from the delta table

-- COMMAND ----------

SELECT * FROM masterdb_dev.demo_db.people@v4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Show data from the delta table at a given timestamp

-- COMMAND ----------

SELECT * FROM masterdb_dev.demo_db.people TIMESTAMP AS OF "2026-03-04T09:09:28.000+00:00"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Delete the delta table data by mistake

-- COMMAND ----------

DELETE FROM masterdb_dev.demo_db.people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####10. Rollback your delete and restore the table to a privious version

-- COMMAND ----------

-- DESCRIBE HISTORY masterdb_dev.demo_db.people;

-- SELECT * FROM masterdb_dev.demo_db.people @v8;

RESTORE TABLE masterdb_dev.demo_db.people TO VERSION AS OF 8;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####11. Read version 1 of the delta table using Dataframe API

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("versionAsOf","10").table("masterdb_dev.demo_db.people").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####12. Read the delta table version at a given timestamp using Dataframe API

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.option("timestampAsOf","2026-03-04T09:12:31.000+00:00").table("masterdb_dev.demo_db.people").display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####13. Restore the delta table to version 1 using the API

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta import DeltaTable;
-- MAGIC
-- MAGIC people_dt = DeltaTable.forName(spark, "masterdb_dev.demo_db.people")
-- MAGIC people_dt.restoreToVersion(3)
-- MAGIC people_dt.toDF().display()
-- MAGIC

-- COMMAND ----------

Select * from masterdb_dev.demo_db.people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2021-2023 ScholarNest Technologies Pvt. Ltd. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://www.scholarnest.com/privacy/">Privacy Policy</a> | 
-- MAGIC <a href="https://www.scholarnest.com/terms/">Terms of Use</a> | <a href="https://www.scholarnest.com/contact/">Contact Us</a>

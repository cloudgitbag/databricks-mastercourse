# Databricks notebook source
class Cleanup():
    def __init__(self):
        pass
  
    def cleanup(self):
        try:
            dbutils.fs.rm("/Volumes/masterdb_dev/demo_db/landing_zone/customers", True)
            dbutils.fs.rm("/Volumes/masterdb_dev/demo_db/landing_zone/invoices", True)
        except:
            pass
        if spark.sql(f"SHOW CATALOGS").filter(f"catalog == 'masterdb_dev'").count() == 1:
            print(f"Dropping the masterdb_dev catalog ...", end='')            
            spark.sql(f"DROP CATALOG masterdb_dev CASCADE")
            print("Done")
    
        if spark.sql(f"SHOW CATALOGS").filter(f"catalog == 'masterdb_qa'").count() == 1:
            print(f"Dropping the qa catalog ...", end='')
            spark.sql(f"DROP CATALOG masterdb_qa CASCADE")
            print("Done")        
        
        if spark.sql(f"SHOW EXTERNAL LOCATIONS").filter(f"name == 'external_data'").count() == 1:
            print(f"Dropping the external-data ...", end='')
            spark.sql(f"DROP EXTERNAL LOCATION `external_data`")
            print("Done")   

        dbutils.fs.rm("/Volumes/masterdb_dev/demo_db/files/dataset_ch8/invoices_2021.csv")
        dbutils.fs.rm("/Volumes/masterdb_dev/demo_db/files/dataset_ch8/invoices_2022.csv")
        dbutils.fs.rm("/Volumes/masterdb_dev/demo_db/files/dataset_ch8//chekpoint", True)     

CL = Cleanup()
CL.cleanup()  

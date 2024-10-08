# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = spark.sql("describe external location `data_zone`").select("url").collect()[0][0]
        print(self.base_dir_data)
        self.base_dir_checkpoint = spark.sql("describe external location `checkpoint`").select("url").collect()[0][0]
        self.db_name = "stream_db"
        self.maxFilesPerTrigger = 1000

# COMMAND ----------

spark.sql("describe external location `data_zone`").select("url").collect()[0][0]

# COMMAND ----------

Conf = Config()
landing_zone = Conf.base_dir_data + "/raw"
print(landing_zone)
checkpoint_base = Conf.base_dir_checkpoint + "/checkpoints" 
print(checkpoint_base)

# Databricks notebook source
# MAGIC %run ./01-config

# COMMAND ----------

class Bronze():
    def __init__(self, env):        
        self.Conf = Config()
        self.landing_zone = self.Conf.base_dir_data + "/raw" 
        self.checkpoint_base = self.Conf.base_dir_checkpoint + "/checkpoints"
        self.catalog = env
        self.db_name = self.Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def consume_user_registration(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "user_id long, device_id long, mac_address string, registration_timestamp double"
        
        df_stream = (spark.readStream
                    .format("json")
                    .schema(self.getSchema())
                    .load(f"{self.landing_zone}/user")
                    )
                        
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/registered_users_bz") \
                                 .outputMode("append") \
                                 .queryName("registered_users_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.registered_users_bz")
          
    def consume_gym_logins(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = "mac_address string, gym bigint, login double, logout double"
        
        df_stream = (spark.readStream
                    .format("csv")
                    .schema(self.getSchema())
                    .load(f"{self.landing_zone}/gym")
                    )
        
        # Use append mode because bronze layer is expected to insert only from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/gym_logins_bz") \
                                 .outputMode("append") \
                                 .queryName("gym_logins_bz_ingestion_stream")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")
            
        if once == True:
            return stream_writer.trigger(availableNow=True).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        else:
            return stream_writer.trigger(processingTime=processing_time).toTable(f"{self.catalog}.{self.db_name}.gym_logins_bz")
        
        
      
        
    def validate(self, sets):
        import time
        start = int(time.time())
        print(f"\nValidating bronz layer records...")
        self.assert_count("registered_users_bz", 5 if sets == 1 else 10)
        self.assert_count("gym_logins_bz", 8 if sets == 1 else 16)
        self.assert_count("kafka_multiplex_bz", 7 if sets == 1 else 13, "topic='user_info'")
        self.assert_count("kafka_multiplex_bz", 16 if sets == 1 else 32, "topic='workout'")
        self.assert_count("kafka_multiplex_bz", sets * 253801, "topic='bpm'")
        print(f"Bronze layer validation completed in {int(time.time()) - start} seconds")                

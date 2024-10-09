# Databricks notebook source
# MAGIC %run ./invoice_streaming_data

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

class AggregationTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/boot_camp"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        dbutils.fs.rm("/user/hive/warehouse/invoices_bz", True)
        dbutils.fs.rm("/user/hive/warehouse/customer_rewards", True)
        spark.sql(f"drop table if exists {DATA_BASE}.{INVOICE_BZ}")
        spark.sql(f"drop table if exists {DATA_BASE}.{CUSTOMER_REWARDS}")
        
        dbutils.notebook.run("./config", timeout_seconds=300)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/invoices_bz", True)
        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint/customer_rewards", True)

        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)
        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertBronze(self, expected_count):
        print(f"\tStarting Bronze validation...", end='')
        actual_count = spark.sql(f"select count(*) from {DATA_BASE}.{INVOICE_BZ}").collect()[0][0]
        print("actual_count:",actual_count)
        print("expected_count:",expected_count)
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def assertGold(self, expected_value):
        print(f"\tStarting Gold validation...", end='')
        actual_value = spark.sql(f"select TotalAmount from {DATA_BASE}.{CUSTOMER_REWARDS} where CustomerCardNo = '2262471989'").collect()[0][0]
        print("actual_count:",actual_value)
        print("expected_count:",expected_value)
        assert expected_value == actual_value, f"Test failed! actual value is {actual_value}"
        print("Done")

    def waitForMicroBatch(self, sleep=30):
        import time
        print(f"\tWaiting for {sleep} seconds...", end='')
        time.sleep(sleep)
        print("Done.")    

    def runTests(self):
        self.cleanTests()
        bzStream = Bronze()
        bzQuery = bzStream.process()
        gdStream = Gold()
        gdQuery = gdStream.process()       

        print("\nTesting first iteration of invoice stream...") 
        self.ingestData(1)
        self.waitForMicroBatch()        
        self.assertBronze(501)
        self.assertGold(36859)
        print("Validation passed.\n")

        print("\nTesting second iteration of invoice stream...") 
        self.ingestData(2)
        self.waitForMicroBatch()        
        self.assertBronze(501+500)
        self.assertGold(36859+20740)
        print("Validation passed.\n")

        print("\nTesting second iteration of invoice stream...") 
        self.ingestData(3)
        self.waitForMicroBatch()        
        self.assertBronze(501+500+590)
        self.assertGold(36859+20740+31959)
        print("Validation passed.\n")

        bzQuery.stop()
        gdQuery.stop()

# COMMAND ----------

aTS = AggregationTestSuite()
aTS.runTests()	

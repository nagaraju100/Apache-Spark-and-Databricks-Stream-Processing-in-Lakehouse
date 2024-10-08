# Databricks notebook source
# MAGIC %run ./03-invoice-stream

# COMMAND ----------

class invoiceStreamWCTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/boot_camp"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists invoice_line_items")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint", True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/invoices", True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/invoices")
        print("Done\n")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/invoices/invoices_{itr}.json", f"{self.base_data_dir}/data/invoices/")
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select count(*) _count from invoice_line_items").collect()[0]["_count"]
        print("actual_count:",actual_count)
        print("expected_count:",expected_count)

        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def runTests(self):
        import time
        sleepTime = 300

        self.cleanTests()
        inStream = invoiceStream()
        sQuery = inStream.process()

        print("Testing first iteration of invoice stream...") 
        self.ingestData(1)
        print(f"\tWaiting for {sleepTime} seconds...") 
        time.sleep(sleepTime)
        self.assertResult(1253)
        print("Validation Passed.\n")

        print("Testing second iteration of invoice stream...") 
        self.ingestData(2)
        print(f"\tWaiting for {sleepTime} seconds...") 
        time.sleep(sleepTime)
        self.assertResult(2510)
        print("Validation Passed.\n")

        print("Testing third iteration of invoice stream...") 
        self.ingestData(3)
        print(f"\tWaiting for {sleepTime} seconds...") 
        time.sleep(sleepTime)
        self.assertResult(3994)
        print("Validation Passed.\n")

        sQuery.stop()
    

# COMMAND ----------

isTS = invoiceStreamWCTestSuite()
isTS.runTests()	

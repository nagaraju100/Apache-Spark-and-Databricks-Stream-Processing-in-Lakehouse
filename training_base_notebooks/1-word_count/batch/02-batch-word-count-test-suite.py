# Databricks notebook source
# MAGIC %run ./01-batch-word-count

# COMMAND ----------

class batchWCTestSuite():
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/boot_camp"

    def cleanTests(self):
        print(f"Starting Cleanup...", end='')
        spark.sql("drop table if exists word_count_table")
        dbutils.fs.rm("/user/hive/warehouse/word_count_table", True)

        dbutils.fs.rm(f"{self.base_data_dir}/chekpoint", True)
        dbutils.fs.rm(f"{self.base_data_dir}/data/text", True)

        dbutils.fs.mkdirs(f"{self.base_data_dir}/data/text")
        print("Done\n")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        dbutils.fs.cp(f"{self.base_data_dir}/datasets/text/text_data_{itr}.txt", f"{self.base_data_dir}/data/text/")
        print("Done")

    def assertResult(self, expected_count):
        print(f"\tStarting validation...", end='')
        actual_count = spark.sql("select sum(count) from word_count_table where substr(word, 1, 1) == 's'").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"
        print("Done")

    def runTests(self):
        self.cleanTests()
        wc = batchWC()

        print("Testing first iteration of batch word count...") 
        self.ingestData(1)
        wc.wordCount()
        self.assertResult(25)
        print("First iteration of batch word count completed.\n")

        print("Testing second iteration of batch word count...") 
        self.ingestData(2)
        wc.wordCount()
        self.assertResult(32)
        print("Second iteration of batch word count completed.\n") 

        print("Testing third iteration of batch word count...") 
        self.ingestData(3)
        wc.wordCount()
        self.assertResult(37)
        print("Third iteration of batch word count completed.\n")
    

# COMMAND ----------

bwcTS = batchWCTestSuite()
bwcTS.runTests()

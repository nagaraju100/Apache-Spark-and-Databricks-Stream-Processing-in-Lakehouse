# Databricks notebook source
class batchWC():
    def __init__(self):
        self.base_data_dir = "/FileStore/tables/boot_camp"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (spark.read
                    .format("text")
                    .option("lineSep", ".")
                    .load(f"{self.base_data_dir}/data/text")
                )
        return lines.select(explode(split(lines.value, " ")).alias("word"))
    
    def getQualityData(self, rawDF):
        from pyspark.sql.functions import trim, lower
        return ( rawDF.select(lower(trim(rawDF.word)).alias("word"))
                        .where("word is not null")
                        .where("word rlike '[a-z]'")
                )
        
    def getWordCount(self, qualityDF):
        return qualityDF.groupBy("word").count()
    
    def overwriteWordCount(self, wordCountDF):
        ( wordCountDF.write
                    .format("delta")
                    .mode("overwrite")
                    .saveAsTable("word_count_table")
        )
    
    def wordCount(self):
        print(f"\tExecuting Word Count...", end='')
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.overwriteWordCount(resultDF)
        print("Done")

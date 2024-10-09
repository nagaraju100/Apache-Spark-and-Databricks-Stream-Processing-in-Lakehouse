# Databricks notebook source
# # Databricks notebook source
# dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
# dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
# dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")


# COMMAND ----------

dbutils.notebook.run("invoice_streaming_data_test_suite", timeout_seconds=30000)

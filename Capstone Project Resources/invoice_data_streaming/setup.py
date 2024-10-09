# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

def create_database():
    spark.sql(f"create database if not exists {DATA_BASE}")

# COMMAND ----------

def create_inovice_table():
    spark.sql(f"""
            CREATE TABLE if not exists spark_catalog.{DATA_BASE}.{INVOICE_BZ} (
    InvoiceNumber STRING,
    CreatedTime BIGINT,
    StoreID STRING,
    PosID STRING,
    CashierID STRING,
    CustomerType STRING,
    CustomerCardNo STRING,
    TotalAmount DOUBLE,
    NumberOfItems BIGINT,
    PaymentMethod STRING,
    TaxableAmount DOUBLE,
    CGST DOUBLE,
    SGST DOUBLE,
    CESS DOUBLE,
    DeliveryType STRING,
    DeliveryAddress STRUCT<AddressLine: STRING, City: STRING, ContactNumber: STRING, PinCode: STRING, State: STRING>,
    InvoiceLineItems ARRAY<STRUCT<ItemCode: STRING, ItemDescription: STRING, ItemPrice: DOUBLE, ItemQty: BIGINT, TotalValue: DOUBLE>>,
    InputFile STRING)
    USING delta
    TBLPROPERTIES (
    'Type' = 'MANAGED',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2')
          """)

# COMMAND ----------

def create_customer_rewards_table():
    spark.sql(f"""
    CREATE TABLE if not exists spark_catalog.{DATA_BASE}.{CUSTOMER_REWARDS} (
    CustomerCardNo STRING,
    TotalAmount DOUBLE,
    TotalPoints DOUBLE)
    USING delta
    TBLPROPERTIES (
    'Type' = 'MANAGED',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '2')
            """)

# COMMAND ----------

create_database()
create_inovice_table()
create_customer_rewards_table()

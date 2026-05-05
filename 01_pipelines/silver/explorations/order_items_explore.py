# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

items_schema = ArrayType(
    StructType(
        [
            StructField("item_id", StringType()),
            StructField("name", StringType()),
            StructField("category", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DecimalType(10, 2)),
            StructField("subtotal", DecimalType(10, 2)),
        ]
    )
)

df_fact_orders = (
    spark.table("01_bronze.orders")
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("order_date", to_date(col("order_timestamp")))
    .withColumn("items_parsed", from_json(col("items"), items_schema))
    .withColumn("item", explode(col("items_parsed")))
    .select(
        "order_id",
        col("item.item_id").alias("item_id"),
        "restaurant_id",
        "order_timestamp",
        "order_date",
        col("item.name").alias("item_name"),
        col("item.category").alias("item_category"),
        col("item.quantity").alias("item_quantity"),
        col("item.unit_price").cast("decimal(10,2)").alias("item_unit_price"),
        col("item.subtotal").cast("decimal(10,2)").alias("item_subtotal")
        
    )
)

display(df_fact_orders)

# COMMAND ----------


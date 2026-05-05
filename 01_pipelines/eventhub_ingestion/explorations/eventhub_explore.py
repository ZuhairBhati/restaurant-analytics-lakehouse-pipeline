# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

EH_NAMESPACE="eventhub_namespace"
EH_NAME="evenhub_name"
EH_CONN_STR="Endpoint"

# COMMAND ----------


KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": "50000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}

# COMMAND ----------

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

display(df_raw, checkpointLocation="/Volumes/dbx_ws_project/00_landing/checkpoints/raw")

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("items", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True)
])

# COMMAND ----------

df_parsed = (
    df_raw
    .withColumn("key_str", col("key").cast("string"))
    .withColumn("value_str", col("value").cast("string"))
    .withColumn("data", from_json("value_str", orders_schema))
    .select("data.*")
    .withColumnRenamed("timestamp", "order_timestamp")
)

display(df_parsed, checkpointLocation="/Volumes/dbx_ws_project/00_landing/checkpoints/parsed_view")

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(name="fact_orders", table_properties={"quality":"silver"})
@dp.expect_all_or_drop({
    "valid_order_id": "order_id IS NOT NULL",
    "valid_order_timestamp": "order_timestamp IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_restaurant_id": "restaurant_id IS NOT NULL",
    "valid_item_count": "item_count > 0",
    "valid_order_status": "order_status IN ('completed', 'pending', 'ready', 'delivered', 'preparing', 'confirm')",
    "valid_payment_method": "payment_method IN ('cash', 'card', 'wallet')",
    "valid_total_amount": "total_amount > 0"
})

def fact_orders():    
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
        .withColumn("order_hour", hour(col("order_timestamp")))
        .withColumn("day_of_week", date_format(col("order_timestamp"), "EEEE"))
        .withColumn("is_weekend", when(col("day_of_week").isin(["Saturday","Sunday"]), True).otherwise(False))
        .withColumn("items_parsed", from_json(col("items"), items_schema))
        .withColumn("item_count", size(col("items_parsed")))
        .select(
            "order_id",
            "order_timestamp",
            "order_date",
            "order_hour",
            "day_of_week",
            "is_weekend",
            "restaurant_id",
            "customer_id",
            "order_type",
            "item_count",
            col("total_amount").cast("decimal(10,2)").alias("total_amount"),
            "payment_method",
            "order_status"
        )
    )

    return df_fact_orders
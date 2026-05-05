# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql. types import *

# COMMAND ----------

df_review_stats = (
    spark.table("02_silver.fact_reviews")
    .groupBy("restaurant_id")
    .agg(
        countDistinct("review_id").alias("total_reviews"),
        round(avg("rating"),2).alias("avg_rating"),
        sum(when(col("rating") == 5,1).otherwise(0)).alias("rating_5_count"),
        sum(when(col("rating") == 4,1).otherwise(0)).alias("rating_4_count"),
        sum(when(col("rating") == 3,1).otherwise(0)).alias("rating_3_count"),
        sum(when(col("rating") == 2,1).otherwise(0)).alias("rating_2_count"),
        sum(when(col("rating") == 1,1).otherwise(0)).alias("rating_1_count"),
        sum(when(col("sentiment") =='positive', 1).otherwise(0)).alias("sentiment_positive_count"),
        sum(when(col("sentiment") =='negative', 1).otherwise(0)).alias("sentiment_negative_count"),
        sum(when(col("sentiment") =='neutral', 1).otherwise(0)).alias("sentiment_neutral_count")
    )
)

display(df_review_stats)

# COMMAND ----------

df_restaurant = spark.table("02_silver.dim_restaurants")

df_restaurant_reviews = (
    df_restaurant.join(df_review_stats, on="restaurant_id", how="left")
    .select(
        "restaurant_id",
        col("name").alias("restaurant_name"),
        "city",
        coalesce(col("total_reviews"), lit(0)).alias("total_reviews"),
        coalesce(col("avg_rating"), lit(0)).alias("avg_rating"),
        coalesce(col("rating_5_count"), lit(0)).alias("rating_5_count"),
        coalesce(col("rating_4_count"), lit(0)).alias("rating_4_count"),
        coalesce(col("rating_3_count"), lit(0)).alias("rating_3_count"),
        coalesce(col("rating_2_count"), lit(0)).alias("rating_2_count"),
        coalesce(col("rating_1_count"), lit(0)).alias("rating_1_count"),
        coalesce(col("sentiment_positive_count"), lit(0)).alias("sentiment_positive_count"),
        coalesce(col("sentiment_negative_count"), lit(0)).alias("sentiment_negative_count"),
        coalesce(col("sentiment_neutral_count"), lit(0)).alias("sentiment_neutral_count")
    )
)

display(df_restaurant_reviews)

# COMMAND ----------


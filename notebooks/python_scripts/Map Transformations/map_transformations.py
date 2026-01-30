# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession.builder.appName("MapTransformations").getOrCreate()

# Sample Data with Map Columns
data = [
(1, "John", {"age": 28, "salary": 50000, "bonus": 5000, "years_of_service": 3}),
(2, "Jane", {"age": 32, "salary": 75000, "bonus": 3000, "years_of_service": 5}),
(3, "Bob", {"age": 25, "salary": 45000, "bonus": 2000, "years_of_service": 2}),
(4, "Alice", {"age": 35, "salary": 90000, "bonus": 7000, "years_of_service": 10}),
(5, "Mike", {"age": 28, "salary": 55000, "years_of_service": 4}),
(6, "Sarah", {"age": 30, "salary": 65000, "bonus": 4000, "years_of_service": 3}),
(7, "David", {"age": 40, "salary": 120000, "bonus": 15000, "years_of_service": 15}),
(8, "Emma", {"age": 27, "salary": 48000, "years_of_service": 2}),
(9, "Chris", {"age": 33, "salary": 85000, "bonus": 8000, "years_of_service": 8}),
(10, "Lisa", {"age": 29, "salary": 52000, "bonus": 3500, "years_of_service": 3})
]
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("attributes", MapType(StringType(), IntegerType()))
])

df = spark.createDataFrame(data, schema)
display(df)

# COMMAND ----------

# df_map_keys_values = df.withColumn("keys", map_keys("attributes")).withColumn("values", map_values("attributes"))
# display(df_map_keys_values)

# key_counts = df_map_keys \
#     .select(explode(col("keys")).alias("key")) \
#     .groupBy("key") \
#     .count() \
#     .orderBy("key")

# print("Key frequencies:")
# key_counts.display()


# exploded_df = df.select(
#     "id",
#     "name",
#     explode(col("attributes")).alias("key", "value")
# )
# display(exploded_df)

df_bonus_safe = df.withColumn("bonus", coalesce(element_at("attributes", "bonus"), lit(0)))
display(df_bonus_safe)


# COMMAND ----------



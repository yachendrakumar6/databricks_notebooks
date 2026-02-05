# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CachePersistCheckpointDemo") \
        .config("spark.sql.shuffle.partitions", "4").getOrCreate()

data = [
    (1, "Alice", "IT", 70000),
    (2, "Bob", "IT", 80000),
    (3, "Charlie", "HR", 60000),
    (4, "David", "HR", 65000),
    (5, "Eva", "Finance", 90000),
    (6, "Frank", "Finance", 85000),
    (7, "Grace", "IT", 75000),
    (8, "Helen", "HR", 62000),
    (9, "Ian", "Finance", 88000),
    (10, "Jane", "IT", 72000)
]

df = spark.createDataFrame(data,["id", "name", "department", "salary"])
display(df)

# COMMAND ----------

filtered_df = df.filter(col("salary") > 70000)

#doesn't work with serverless compute
filtered_df.cache()
filtered_df.count()      # materialize cache
filtered_df.show()

# COMMAND ----------

agg_df = df.groupBy("department") \
           .avg("salary")

#doesn't work with serverless compute
agg_df.persist(StorageLevel.MEMORY_AND_DISK)
agg_df.count()
agg_df.show()


# COMMAND ----------

spark.sparkContext.setCheckpointDir("CachePersistCheckpointDemo")

filtered_df = df.filter(col("salary") > 70000)
filtered_df = filtered_df.checkpoint()

filtered_df.count()
filtered_df.show()

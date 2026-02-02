# Databricks notebook source
# MAGIC %md
# MAGIC ========================================
# MAGIC repartition → Shuffle everything evenly
# MAGIC
# MAGIC repartitionByRange → Shuffle by ordered ranges
# MAGIC
# MAGIC coalesce → Merge partitions cheaply
# MAGIC
# MAGIC hint → Suggest execution strategy
# MAGIC
# MAGIC ----------------------------------------------------------------------
# MAGIC | Method                 | What it does in ONE LINE                  |
# MAGIC | ---------------------- | ----------------------------------------- |
# MAGIC | `repartition()`        | **Reshuffles everything evenly**          |
# MAGIC | `repartitionByRange()` | **Groups data by value ranges**           |
# MAGIC | `coalesce()`           | **Merges existing partitions (cheap)**    |
# MAGIC | `hint()`               | **Gives Spark a suggestion (not forced)** |
# MAGIC ----------------------------------------------------------------------
# MAGIC ==========================================
# MAGIC --------------------------------------------
# MAGIC | Situation       | Use                    |
# MAGIC | --------------- | ---------------------- |
# MAGIC | Before join     | `repartition()`        |
# MAGIC | Time-based data | `repartitionByRange()` |
# MAGIC | Before writing  | `coalesce()`           |
# MAGIC | Slow join       | `hint()`               |
# MAGIC --------------------------------------------
# MAGIC ---
# MAGIC ## Spark Partitioning Methods
# MAGIC
# MAGIC | Method                 | What it does in ONE LINE                  |
# MAGIC |------------------------|-------------------------------------------|
# MAGIC | `repartition()`        | **Reshuffles everything evenly**          |
# MAGIC | `repartitionByRange()` | **Groups data by value ranges**           |
# MAGIC | `coalesce()`           | **Merges existing partitions (cheap)**    |
# MAGIC | `hint()`               | **Gives Spark a suggestion (not forced)** |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Each Method
# MAGIC
# MAGIC | Situation         | Use                    |
# MAGIC |-------------------|------------------------|
# MAGIC | Before join       | `repartition()`        |
# MAGIC | Time-based data   | `repartitionByRange()` |
# MAGIC | Before writing    | `coalesce()`           |
# MAGIC | Slow join         | `hint()`               |
# MAGIC
# MAGIC ---
# MAGIC Methods that works
# MAGIC repartition() → shuffle cards 🃏
# MAGIC
# MAGIC repartitionByRange() → sort cards by number 🔢
# MAGIC
# MAGIC coalesce() → put two decks together 📚
# MAGIC
# MAGIC hint() → tell dealer how to deal

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("PartitioningDemo").getOrCreate()

emp_data = [
    (1, "Alice",   "IT",    "IN",  95000, "2022-01-10", 101),
    (2, "Bob",     "IT",    "US", 105000, "2021-03-15", 102),
    (3, "Charlie", "HR",    "IN",  60000, "2020-07-01", 103),
    (4, "David",   "HR",    "IN",  62000, "2023-02-20", 103),
    (5, "Eva",     "FIN",   "US", 120000, "2019-11-11", 104),
    (6, "Frank",   "IT",    "IN",  98000, "2021-09-09", 101),
    (7, "Grace",   "FIN",   "UK", 110000, "2022-05-30", 104),
    (8, "Helen",   "IT",    "IN",  97000, "2023-08-01", 102),
    (9, "Ivy",     "HR",    "IN",  61000, "2020-12-12", 103),
    (10,"Jack",    "IT",    "US", 115000, "2018-04-25", 101),
    (11,"Kevin",   "IT",    "IN",  99000, "2024-01-01", 101),
    (12,"Laura",   "FIN",   "IN", 108000, "2022-10-10", 104)
]

emp_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("emp_name", StringType()),
    StructField("dept", StringType()),
    StructField("country", StringType()),
    StructField("salary", IntegerType()),
    StructField("join_date", StringType()),
    StructField("dept_id", IntegerType())
])

emp_df = spark.createDataFrame(emp_data, emp_schema) \
    .withColumn("join_date", to_date("join_date"))
print("Employee Table")
display(emp_df)

dept_data = [
    (101, "Engineering", "Bangalore"),
    (102, "Platform",    "Hyderabad"),
    (103, "Human Resources", "Chennai"),
    (104, "Finance",     "Mumbai")
]

dept_schema = StructType([
    StructField("dept_id", IntegerType()),
    StructField("dept_name", StringType()),
    StructField("location", StringType())
])

dept_df = spark.createDataFrame(dept_data, dept_schema)
print("Department Table")
display(dept_df)

# COMMAND ----------

print("repartition() method")
df_repartition = emp_df.repartition(4, "dept_id")
display(df_repartition)

print("repartitionByRange() method")
repartitionByRange_df = emp_df.repartitionByRange(4, "join_date")
display(repartitionByRange_df)

print("hint() method")
df_hint = emp_df.join(dept_df.hint("broadcast"), "dept_id")
display(df_hint)

print("coalesce() method")
df_coalesce = emp_df.filter(col("country") == "IN").coalesce(2)
display(df_coalesce)

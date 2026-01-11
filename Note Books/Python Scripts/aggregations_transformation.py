# Databricks notebook source
data = [
    (1, "IT", 60000),
    (2, "IT", 70000),
    (3, "HR", 40000),
    (4, "HR", 45000),
    (5, "Finance", 80000)
]

columns = ["emp_id", "dept", "salary"]
df = spark.createDataFrame(data, columns)
# display(df)

# agg1 = df.groupBy("dept").agg({"salary": "avg"})
# display(agg1)

rollup = df.rollup("dept").agg({"salary": "sum"})
display(rollup)





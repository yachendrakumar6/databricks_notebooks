# Databricks notebook source
from pyspark.sql.functions import col

data = spark.read.format("csv").option("header", True).option("inferSchema", "true").load("/Volumes/workspace/default/adventure_works/HumanResource/HumanResources_Department.csv")
#df = data.toDF("DepartmentID", "Name", "GroupName", "ModifiedDate")
display(df)
#df_filtered = df.filter(col("_c2") == "Research and Development")
#display(df_filtered)

# COMMAND ----------

from pyspark.sql.functions import col, trim
#==========================
#using trim and col functions
#==========================

df_filtered1 = df.filter(trim(col("GroupName")) == "Research and Development")
display(df_filtered1)

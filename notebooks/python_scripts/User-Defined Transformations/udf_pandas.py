# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("UDFs_DeepDive").getOrCreate()
data = [
    (1, "Alice", 28, "IT", 60000, ["Java", "Spark"], "India"),
    (2, "Bob", 35, "IT", 85000, ["Python", "Spark", "AWS"], "USA"),
    (3, "Charlie", 40, "HR", 50000, ["Excel"], "India"),
    (4, "David", 25, "IT", None, ["Python"], "Canada"),
    (5, "Eva", 32, "Finance", 95000, ["SQL", "PowerBI"], "UK"),
    (6, "Frank", 45, "Finance", 120000, ["Python", "ML"], "USA"),
    (7, "Grace", None, "HR", 48000, [], "India"),
    (8, "Helen", 29, "IT", 72000, ["Java", "AWS"], "Germany"),
    (9, "Ian", 38, "IT", 110000, ["Scala", "Spark"], "USA"),
    (10, "Jane", 26, "Finance", 58000, ["Excel", "SQL"], "India")
]
schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("emp_name", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("salary", IntegerType()),
    StructField("skills", ArrayType(StringType())),
    StructField("country", StringType())
])
df = spark.createDataFrame(data, schema)
display(df)



# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def salary_band(salary):
    if salary is None:
        return "unknown"
    elif salary >= 100000:
        return "high"
    elif salary >= 70000:
        return "medium"
    else:
        return "low"

salary_band_udf = udf(salary_band, StringType())

df_udf = df.withColumn("salary_band", salary_band_udf("salary"))
display(df_udf)


# COMMAND ----------

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(IntegerType())
def tax_calculator(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for salary in it:
        yield salary.fillna(0) * 0.20

df.withColumn("tax", tax_calculator("salary")).show()


# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

def enrich_employee(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf[pdf["salary"].notna()]
    pdf["salary_usd"] = pdf["salary"] * 1.0
    pdf["seniority"] = pdf["age"].apply(
        lambda x: "Senior" if x and x >= 35 else "Junior"
    )
    return pdf

df.mapInPandas(enrich_employee, df.schema).show()


# COMMAND ----------

import pandas as pd

@pandas_udf(StringType())
def grade(age: pd.Series, salary: pd.Series) -> pd.Series:
    return pd.Series([
        "A" if (a and a > 30 and s and s > 80000) else "B"
        for a, s in zip(age, salary)
    ])

df.withColumn("grade", grade("age", "salary")).show()


# COMMAND ----------

import pandas as pd
from typing import Iterator

@pandas_udf(IntegerType())
def tax_calculator(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for salary in it:
        yield salary.fillna(0) * 0.20

df.withColumn("tax", tax_calculator("salary")).show()


# COMMAND ----------

import pandas as pd
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def explode_skills(pdf_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in pdf_iter:
        rows = []
        for _, r in pdf.iterrows():
            for skill in r["skills"]:
                rows.append({
                    "emp_id": r["emp_id"],
                    "emp_name": r["emp_name"],
                    "skill": skill
                })
        yield pd.DataFrame(rows)


schema_exploded = StructType([
    StructField("emp_id", IntegerType()),
    StructField("emp_name", StringType()),
    StructField("skill", StringType())
])

df_mapInPandas = df.mapInPandas(explode_skills, schema_exploded)
display(df_mapInPandas)

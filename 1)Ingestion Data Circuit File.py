# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1stor/raw

# COMMAND ----------

circuit_df = spark.read.option("Header",True).csv("dbfs:/mnt/formula1stor/raw/circuits.csv")


# COMMAND ----------

type(circuit_df)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

#print Schema
circuit_df.printSchema()

# COMMAND ----------

circuit_df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------



# COMMAND ----------

circuit_df = spark.read\
    .option("Header",True)\
    .option("inferSchema",True)\
    .csv("dbfs:/mnt/formula1stor/raw/circuits.csv")


# COMMAND ----------

#print Schema
circuit_df.printSchema()

# COMMAND ----------



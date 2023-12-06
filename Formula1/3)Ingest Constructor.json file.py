# Databricks notebook source
# MAGIC %md
# MAGIC Step1- Read Json File using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorID INT,constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/formula1stor/raw/constructors.json")

# COMMAND ----------

#Print Schema
constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------



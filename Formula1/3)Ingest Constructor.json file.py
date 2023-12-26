# Databricks notebook source
# MAGIC %md
# MAGIC Step1- Read Json File using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorID INT,constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/adlsgen2formula1/raw/constructors.json")

# COMMAND ----------

#Print Schema
constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/adlsgen2formula1/processed/constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/adlsgen2formula1/processed/constructors"))

# COMMAND ----------



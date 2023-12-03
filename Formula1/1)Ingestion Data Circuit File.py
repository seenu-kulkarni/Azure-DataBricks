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

circuit_schema = StructType(fields=[
    StructField("circuitid",IntegerType(),False),
    StructField("circuitref",StringType(),True),
    StructField("name",StringType(),True),
    StructField("location",StringType(),True),
    StructField("country",StringType(),True),
    StructField("lat",DoubleType(),True),
    StructField("lng",DoubleType(),True),
    StructField("alt",IntegerType(),True),
    StructField("url",StringType(),True),
])

# COMMAND ----------

circuit_df = spark.read\
    .option("Header",True)\
    .schema(circuit_schema)\
    .csv("dbfs:/mnt/formula1stor/raw/circuits.csv")


# COMMAND ----------

#print Schema
circuit_df.printSchema()

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Select Required Column in the Circuit Data Frame

# COMMAND ----------

circuits_selected_df = circuit_df.select("circuitid","circuitref","name","location","country","lat","lng","alt")

# COMMAND ----------

circuits_selected_df = circuit_df.select(circuit_df.circuitid,circuit_df.circuitref,circuit_df.name,circuit_df.location,circuit_df.country,circuit_df.lat,circuit_df.lng,circuit_df.alt)

# COMMAND ----------

circuits_selected_df = circuit_df.select(circuit_df["circuitid"],circuit_df["circuitref"],circuit_df["name"],circuit_df["location"],circuit_df["country"],circuit_df["lat"],circuit_df["lng"],circuit_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuit_df.select(col("circuitid"),col("circuitref"),col("name"),col("location"),col("country").alias("Country_location"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Rename Columns

# COMMAND ----------

circuit_renamed_df = circuits_selected_df.withColumnRenamed("circuitid","circuit_id") \
.withColumnRenamed("circuitref","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Add Ingestion Date to the Data Frame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuit_final_df = circuit_renamed_df.withColumn("IngestionDate",current_timestamp())
   # .withColumn("env",lit("Development"))

# COMMAND ----------

display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write Data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").parquet("/mnt/formula1stor/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1stor/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1stor/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------



# Databricks notebook source
client_id='cacb9f0d-6f0c-4689-a7b5-264b50c830e2'
tenent_id = 'ac81ebfc-6e2c-422a-a2bf-b91938051c4b'
client_secret = 'M6i8Q~zglJB5u~4TuMWwu-vVnLyiKwbuAV3Sjbwh'

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsgen2formula1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adlsgen2formula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adlsgen2formula1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.adlsgen2formula1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adlsgen2formula1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenent_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenent_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@adlsgen2formula1.dfs.core.windows.net/",
  mount_point = "/mnt/adlsgen2formula1/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adlsgen2formula1/raw"))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@adlsgen2formula1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("/mnt/adlsgen2formula1/raw/circuits.csv"))

# COMMAND ----------



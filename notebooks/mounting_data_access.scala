// Databricks notebook source
// Creating a directory in DBFS to mount the data lake
dbutils.fs.mkdirs("/mnt/data")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt")

// COMMAND ----------

dbutils.secrets.list(scope="datalake-pipeline-secrets")

// COMMAND ----------

// Defining the secret variables from Azure app registration
val app_id = dbutils.secrets.get("datalake-pipeline-secrets", "app-id")
val client_secret = dbutils.secrets.get("datalake-pipeline-secrets", "client-credentials")
val tennant_id = dbutils.secrets.get("datalake-pipeline-secrets", "tennant-id")
val storage_account = "datalakealurateste"
val container = "imoveis"

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> app_id,
  "fs.azure.account.oauth2.client.secret" -> client_secret,
  "fs.azure.account.oauth2.client.endpoint" -> s"https://login.microsoftonline.com/$tennant_id/oauth2/token")

// Mounting the Azure Data Lake in Databricks
dbutils.fs.mount(
  source = s"abfss://$container@$storage_account.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)


// COMMAND ----------

// MAGIC %python
// MAGIC # Checking if the mount was successful
// MAGIC dbutils.fs.ls("/mnt/data")

// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/data/inbound")

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading data in inbound layer

// COMMAND ----------

val path = "dbfs:/mnt/data/inbound/properties_raw_data.json"
val data = spark.read.json(path)

// COMMAND ----------

display(data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removing unwanted columns (images and users)

// COMMAND ----------

val advertisement_data = data.drop("imagens", "usuario")
display(advertisement_data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Creating id column

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = advertisement_data.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC # Saving data in bronze layer

// COMMAND ----------

val path = "dbfs:/mnt/data/bronze/properties_dataset"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

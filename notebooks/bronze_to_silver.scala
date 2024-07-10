// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/data/bronze")

// COMMAND ----------

// MAGIC %md
// MAGIC # Reading bronze layer data

// COMMAND ----------

val path = "dbfs:/mnt/data/bronze/properties_dataset/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Converting each json field into a column

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")
)

// COMMAND ----------

val detailed_data = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(detailed_data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removing brackets from some columns
// MAGIC Some columns are formatted as arrays, we are getting the first element of that array out, and casting them as ints

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val cleaned_data = 
    detailed_data
      .withColumn("area_total", element_at(col("area_total"), 1).cast("int"))
      .withColumn("area_util", element_at(col("area_util"), 1).cast("int"))
      .withColumn("banheiros", element_at(col("banheiros"), 1))
      .withColumn("quartos", element_at(col("quartos"), 1))
      .withColumn("suites", element_at(col("suites"), 1))
      .withColumn("vaga", element_at(col("vaga"), 1))
      .withColumn("valores", element_at(col("valores"), 1)) // It was decided to keep the "valores" column as a struct, instead of exploding its values in other columns

display(cleaned_data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Removing unwanted columns (characteristics and copiled address)

// COMMAND ----------

val df_silver = cleaned_data.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

// MAGIC %md
// MAGIC # Saving in silver layer

// COMMAND ----------

val path = "dbfs:/mnt/data/silver/properties_dataset"
df_silver.write.format("delta").mode("overwrite").save(path)

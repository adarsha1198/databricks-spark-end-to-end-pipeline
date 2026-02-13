# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC establish connection

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlsstorageaccnow.dfs.core.windows.net","pLUYO4n/I6z2qLRM+WRoVOa9CFStIOQ+J4Xou9gynczAZqBYSp879rZ6WgrfYPEawyXRmtCoxw/g+AStOIvAQA==")

# COMMAND ----------

# MAGIC %md
# MAGIC get file name

# COMMAND ----------

filename = dbutils.fs.ls("abfss://bronze.@adlsstorageaccnow.dfs.core.windows.net")[0].name


# COMMAND ----------

# MAGIC %md
# MAGIC load the file

# COMMAND ----------

df=spark.read.parquet(f"abfss://bronze.@adlsstorageaccnow.dfs.core.windows.net/{filename}")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC capitalize column

# COMMAND ----------

df=df.withColumn('country',initcap(df['country']))
df=df.withColumn('ProductCategory',initcap(df['ProductCategory']))
df=df.withColumn('ProductName',initcap(df['ProductName']))
df=df.withColumn('SalesRegion',initcap(df['SalesRegion']))
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC drop missing values

# COMMAND ----------

df=df.dropna(how='all')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC remove duplicates

# COMMAND ----------

df=df.dropDuplicates()
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC save data in silver

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','abfss://silver.@adlsstorageaccnow.dfs.core.windows.net/').save()
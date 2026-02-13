# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

incremental_pipeline=dbutils.widgets.text("incremental pipeline","0")

# COMMAND ----------

dbutils.widgets.get("incremental pipeline")

# COMMAND ----------

spark.conf.set('fs.azure.account.key.adlsstorageaccnow.dfs.core.windows.net',"pLUYO4n/I6z2qLRM+WRoVOa9CFStIOQ+J4Xou9gynczAZqBYSp879rZ6WgrfYPEawyXRmtCoxw/g+AStOIvAQA==")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ProductID,
# MAGIC        first(ProductName)  AS ProductName,
# MAGIC        first(ProductCategory) AS ProductCategory,
# MAGIC        first(UnitPrice)       AS UnitPrice
# MAGIC FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`
# MAGIC GROUP BY ProductID;
# MAGIC

# COMMAND ----------

df_src=spark.sql('''SELECT ProductID,
       first(ProductName)  AS ProductName,
       first(ProductCategory) AS ProductCategory,
       first(UnitPrice)       AS UnitPrice
FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`
GROUP BY ProductID;
''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC getting gold layer data

# COMMAND ----------

from delta.tables import DeltaTable
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Product'
if DeltaTable.isDeltaTable(spark,path):
  df_sink = spark.sql("""select dim_product_key, ProductID, ProductName, ProductCategory, UnitPrice from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Product`""")
  df_sink.display()
else:
  df_sink = spark.createDataFrame([], schema='dim_product_key int,ProductID int,ProductName string,ProductCategory string,UnitPrice double')
  df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC old and new records left join

# COMMAND ----------

df = df_src.join(df_sink, df_src.ProductID == df_sink.ProductID, 'left').select(df_src['ProductID'],df_src['ProductName'],df_src['ProductCategory'],df_src['UnitPrice'],df_sink['dim_product_key'])
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC old records

# COMMAND ----------

df_old=df.filter(df.dim_product_key.isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC new records

# COMMAND ----------

df_new=df.filter(df.dim_product_key.isNull())
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC get max dim_customer key

# COMMAND ----------


if incremental_pipeline == 0 or incremental_pipeline == "0":
    max_value = 1
else:
    result = df_old.select(max(col("dim_product_key")).alias("max_id")).collect()[0]
    max_value = result.max_id if result.max_id is not None else 1

# COMMAND ----------

# MAGIC %md
# MAGIC creating surrogate keys

# COMMAND ----------


df_new=df_new.withColumn('dim_product_key',max_value+monotonically_increasing_id())
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC union of old with new

# COMMAND ----------

df=df_old.union(df_new)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD type 1 - upsert

# COMMAND ----------

from delta.tables import *

table_name='dim_product'
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Product'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.alias("old").merge(df.alias("new"), "old.dim_product_key = new.dim_product_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Product`
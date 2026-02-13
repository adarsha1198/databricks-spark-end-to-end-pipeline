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
# MAGIC SELECT DISTINCT(SalesRegion) AS SalesRegion
# MAGIC FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`;
# MAGIC

# COMMAND ----------

df_src=spark.sql('''SELECT DISTINCT(SalesRegion) AS SalesRegion
FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`;
''')
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC getting gold layer data

# COMMAND ----------

from delta.tables import DeltaTable
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_SalesRegion'
if DeltaTable.isDeltaTable(spark,path):
  df_sink = spark.sql("""select dim_SalesRegion_key,SalesRegion from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_SalesRegion`""")
  df_sink.display()
else:
  df_sink = spark.createDataFrame([], schema='dim_SalesRegion_key int,SalesRegion string')
  df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC old and new records left join

# COMMAND ----------

df = df_src.join(df_sink, df_src['SalesRegion']==df_sink['SalesRegion'],'left').select(df_src['SalesRegion'],df_sink['dim_SalesRegion_key'])
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC old records

# COMMAND ----------

df_old=df.filter(df.dim_SalesRegion_key.isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC new records

# COMMAND ----------

df_new=df.filter(df.dim_SalesRegion_key.isNull())
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC get max dim_customer key

# COMMAND ----------


if incremental_pipeline == 0 or incremental_pipeline == "0":
    max_value = 1
else:
    result = df_old.select(max(col("dim_SalesRegion_key")).alias("max_id")).collect()[0]
    max_value = result.max_id if result.max_id is not None else 1

# COMMAND ----------

# MAGIC %md
# MAGIC creating surrogate keys

# COMMAND ----------


df_new=df_new.withColumn('dim_SalesRegion_key',max_value+monotonically_increasing_id())
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

table_name='dim_SalesRegion'
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_SalesRegion'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.alias("old").merge(df.alias("new"), "old.dim_SalesRegion_key = new.dim_SalesRegion_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_SalesRegion`
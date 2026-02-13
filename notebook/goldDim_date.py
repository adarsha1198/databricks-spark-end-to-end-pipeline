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
# MAGIC SELECT DISTINCT(OrderDate) AS OrderDate
# MAGIC FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`;
# MAGIC

# COMMAND ----------

df_src=spark.sql('''SELECT DISTINCT(OrderDate) AS OrderDate
FROM parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`;
''')
df_src=df_src.withColumn("Year", year(df_src.OrderDate)).withColumn("Month", month(df_src.OrderDate)).withColumn("Day", day(df_src.OrderDate))
df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC getting gold layer data

# COMMAND ----------

from delta.tables import DeltaTable
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Date'
if DeltaTable.isDeltaTable(spark,path):
  df_sink = spark.sql("""select dim_date_key,OrderDate,Year,Month,Day from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Date`""")
  df_sink.display()
else:
  df_sink = spark.createDataFrame([], schema='dim_date_key int,OrderDate date,Year int,Month int,Day int')
  df_sink.display()

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC old and new records left join

# COMMAND ----------

df = df_src.join(df_sink, df_src['OrderDate']==df_sink['OrderDate'],'left').select(df_src['OrderDate'],df_src['Year'],df_src['Month'],df_src['Day'],df_sink['dim_date_key'])
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC old records

# COMMAND ----------

df_old=df.filter(df.dim_date_key.isNotNull())
df_old.display()


# COMMAND ----------

# MAGIC %md
# MAGIC new records

# COMMAND ----------

df_new=df.filter(df.dim_date_key.isNull())
df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC get max dim_customer key

# COMMAND ----------


if incremental_pipeline == 0 or incremental_pipeline == "0":
    max_value = 1
else:
    result = df_old.select(max(col("dim_date_key")).alias("max_id")).collect()[0]
    max_value = result.max_id if result.max_id is not None else 1

# COMMAND ----------

# MAGIC %md
# MAGIC creating surrogate keys

# COMMAND ----------


df_new=df_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())
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
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Date'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.alias("old").merge(df.alias("new"), "old.dim_date_key = new.dim_date_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
display(dbutils.fs.ls(path))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Date`
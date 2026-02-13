# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set('fs.azure.account.key.adlsstorageaccnow.dfs.core.windows.net',"pLUYO4n/I6z2qLRM+WRoVOa9CFStIOQ+J4Xou9gynczAZqBYSp879rZ6WgrfYPEawyXRmtCoxw/g+AStOIvAQA==")

# COMMAND ----------

# MAGIC %md
# MAGIC load all dimension

# COMMAND ----------

df_customer=spark.sql("""select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Customer`""")
df_date=spark.sql("""select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Date`""")
df_product=spark.sql("""select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_Product`""")
df_salesRegion=spark.sql("""select * from delta.`abfss://gold@adlsstorageaccnow.dfs.core.windows.net/dim_SalesRegion`""")



# COMMAND ----------

# MAGIC %md
# MAGIC load silver data

# COMMAND ----------

df_silver=spark.sql("""select * from parquet.`abfss://silver@adlsstorageaccnow.dfs.core.windows.net/`""")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC create fact table

# COMMAND ----------

df_fact=df_silver.join(df_customer,df_silver.CustomerID==df_customer.CustomerID,"left").join(df_date,df_silver.OrderDate==df_date.OrderDate,"left").join(df_product,df_silver.ProductID==df_product.ProductID,"left").join(df_salesRegion,df_silver.SalesRegion==df_salesRegion.SalesRegion,"left")
df_fact.display()
dff_fact_real=df_fact.select(df_fact['dim_customer_key'],df_fact['dim_product_key'],df_fact['dim_date_key'],df_fact['dim_SalesRegion_key'],df_silver['UnitPrice'],df_fact['TotalPrice'],df_fact['Quantity'],)
dff_fact_real.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD type 1 - upsert

# COMMAND ----------

from delta.tables import DeltaTable

table_name='Fact_sales'
path='abfss://gold@adlsstorageaccnow.dfs.core.windows.net/Fact_sales'

if DeltaTable.isDeltaTable(spark,path):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.alias("old").merge(dff_fact_real.alias("new"), "old.dim_SalesRegion_key = new.dim_SalesRegion_key and old.dim_customer_key = new.dim_customer_key and old.dim_date_key = new.dim_date_key and old.dim_product_key = new.dim_product_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    dff_fact_real.display()
else:
    dff_fact_real.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
    print('nahi hua')
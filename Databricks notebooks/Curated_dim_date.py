# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create Flag parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md 
# MAGIC # CREATING DIMENSIONAL MODEL

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Fetch Relative `columns`

# COMMAND ----------

df_src = spark.sql('''
select distinct(Date_ID) as Date_ID
from parquet.`abfss://conformed@carsalesadls.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Dim_model sink - Initial and Incremental (Just bring the schema if table NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.curated.dim_date') :  

    df_sink = spark.sql('''
    select dim_date_key, Date_ID
    from cars_catalog.curated.dim_date
    ''')
    

else : 
    
    df_sink = spark.sql('''
    select 1 as dim_date_key, Date_ID
    from parquet.`abfss://conformed@carsalesadls.dfs.core.windows.net/carsales`
    where 1=0
    ''')



# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Filtering new records and old records (JOIN)

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Date_ID == df_sink.Date_ID, 'left').select(df_src.Date_ID, df_sink.dim_date_key)  

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## df_filter_old

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------


df_filter_old = df_filter.filter(col('dim_date_key').isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_key').isNull()).select (df_src.Date_ID)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Surrogate key

# COMMAND ----------

if (incremental_flag == '0') :
    max_value = 1

else :
    max_value_df = spark.sql("select max(dim_date_key) from cars_catalog.curated.dim_date")
    max_value = max_value_df.collect()[0][0]+1


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create surrogate key column and add the max surrogate key 

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Final DF - df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md 
# MAGIC # SCD Type 1 (Upsert)

# COMMAND ----------

from delta.tables import DeltaTable


# COMMAND ----------

#incremental run
if spark.catalog.tableExists('cars_catalog.curated.dim_date') :
    delta_tbl = DeltaTable.forPath(spark, "abfss://curated@carsalesadls.dfs.core.windows.net/dim_date")
    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_date_key = src.dim_date_key")\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
    

#initial run
else : 
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://curated@carsalesadls.dfs.core.windows.net/dim_date")\
        .saveAsTable("cars_catalog.curated.dim_date")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.curated.dim_date

# COMMAND ----------


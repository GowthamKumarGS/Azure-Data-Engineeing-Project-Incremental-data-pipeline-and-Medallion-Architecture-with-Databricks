# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FACT TABLE 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Conformed data

# COMMAND ----------

df_conformed = spark.sql("SELECT * FROM PARQUET.`abfss://conformed@carsalesadls.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_conformed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading all the Dimensions

# COMMAND ----------

df_dealer = spark.sql("SELECT * FROM cars_catalog.curated.dim_dealer")

df_branch = spark.sql("SELECT * FROM cars_catalog.curated.dim_branch")

df_model = spark.sql("SELECT * FROM cars_catalog.curated.dim_model")

df_date = spark.sql("SELECT * FROM cars_catalog.curated.dim_date")

# COMMAND ----------

df_fact = df_conformed.join(df_branch, df_conformed['Branch_ID'] == df_branch['Branch_ID'],how="left")\
                       .join(df_dealer, df_conformed['Dealer_ID'] == df_dealer['Dealer_ID'],how="left")\
                       .join(df_model, df_conformed['Model_ID'] == df_model['Model_ID'],how="left")\
                       .join(df_date, df_conformed['Date_ID'] == df_date['Date_ID'],how="left")\
                       .select(df_conformed['Revenue'], df_conformed['Units_Sold'], df_conformed['revperunit'], df_branch['dim_branch_key'], df_dealer['dim_dealer_key'], df_model['dim_mod_key'], df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Writing fact table 

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists('factsales') :
    deltatbl = DeltaTable.forName(spark, 'cars_catalog.curated.factsales')

    deltatbl.alias('trg').merge(df_fact.alias('src'), 'trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_mod_key = src.dim_mod_key and trg.dim_date_key = src.dim_date_key')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else : 
    df_fact.write.format('delta')\
            .mode('overwrite')\
            .option("path", "abfss://curated@carsalesadls.dfs.core.windows.net/factsales")\
            .saveAsTable('cars_catalog.curated.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.curated.factsales

# COMMAND ----------


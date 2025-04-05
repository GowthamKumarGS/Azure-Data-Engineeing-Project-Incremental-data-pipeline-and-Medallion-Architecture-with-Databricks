# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format('parquet')\
        .option('inferSchema', True)\
        .load('abfss://raw@carsalesadls.dfs.core.windows.net/rawdata') 

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.withColumn('Model_name', split(col('Model_ID'),'-')[0])


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

df.withColumn('Units_Sold', col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df = df.withColumn('revperunit', col('Revenue')/col('Units_Sold'))

# COMMAND ----------

df.display()

# COMMAND ----------

display(df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold', ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Data Writing 

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path','abfss://conformed@carsalesadls.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Querying the conformed data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`abfss://conformed@carsalesadls.dfs.core.windows.net/carsales`

# COMMAND ----------


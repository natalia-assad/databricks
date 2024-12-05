# Databricks notebook source
import delta

# COMMAND ----------

df_full = spark.read.format("csv") \
    .option("header", True) \
    .load("/Volumes/raw/transactions/full-load")

df_full.coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze.transactions.full")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transactions.`full`

# COMMAND ----------

df_cdc = spark.read.format("csv") \
    .option("header", True) \
    .load("/Volumes/raw/transactions/cdc")

df_cdc.createOrReplaceTempView("transactions")

query = '''
select * from transactions
qualify row_number() over (partition by transaction_id order by action_timestamp desc) = 1 '''

df_cdc_unique = spark.sql(query)

df_cdc_unique.display()

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark,"bronze.transactions.full")

#UPSERT
(bronze.alias('b')
    .merge(df_cdc_unique.alias('cdc'), 'b.transaction_id = cdc.transaction_id')
    .whenMatchedUpdateAll(condition="cdc.action_type='UPDATE'")
    .whenNotMatchedInsertAll(condition="cdc.action_type='INSERT' or cdc.action_type='UPDATE' ")
    .execute()
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transactions.full

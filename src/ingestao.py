# Databricks notebook source
# DBTITLE 1,IMPORTS
import delta

# COMMAND ----------

# DBTITLE 1,SETUP
catalog = "bronze"
schema = "transactions"
tablename = dbutils.widgets.get("tablename")
field_id =  dbutils.widgets.get("field_id")
timestamp_field =  dbutils.widgets.get("timestamp_field")

# COMMAND ----------

def table_exists(catalog,database,table):
    count = (spark.sql(f"show tables from {catalog}.{database}")
             .filter(f"database='{database}' and tableName='{tablename}'")
             .count())
    return count == 1

# COMMAND ----------

if not table_exists(catalog,schema,tablename):
    print("Criando tabela")
    df_full = spark.read.format("csv") \
        .option("header", True) \
        .load(f"/Volumes/raw/transactions/{tablename}")

    df_full.coalesce(1).write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.`{tablename}`")
else:
    print("Tabela já existente")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transactions.full_transaction

# COMMAND ----------

# DBTITLE 1,CDC
df_cdc = spark.read.format("csv") \
    .option("header", True) \
    .load(f"/Volumes/raw/{schema}/cdc")

df_cdc.createOrReplaceTempView(f"view_{schema}")

query = f'''
select * from view_{schema}
qualify row_number() over (partition by {field_id} order by {timestamp_field} desc) = 1'''

df_cdc_unique = spark.sql(query)

df_cdc_unique.display()

# COMMAND ----------

bronze = delta.DeltaTable.forName(spark,f"{catalog}.{schema}.`{tablename}`")

#UPSERT
(bronze.alias('b')
    .merge(df_cdc_unique.alias('cdc'), f'b.{field_id} = cdc.{field_id}')
    .whenMatchedUpdateAll(condition="cdc.action_type='UPDATE'")
    .whenNotMatchedInsertAll(condition="cdc.action_type='INSERT' or cdc.action_type='UPDATE' ")
    .execute()
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transactions.full_transaction

# COMMAND ----------



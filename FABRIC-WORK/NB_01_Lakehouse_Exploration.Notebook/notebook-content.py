# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0b0c12c-42f4-483a-91ad-12d7335b7b2e",
# META       "default_lakehouse_name": "seg_lh_core",
# META       "default_lakehouse_workspace_id": "18f6a522-68ef-48fc-b8a7-8cac036e6c35",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0b0c12c-42f4-483a-91ad-12d7335b7b2e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get("spark.sql.catalogImplementation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark.conf.get("spark.sql.warehouse.dir"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# READ CSV FROM FILES / 01_RAW

df = spark.read.option("header", "true").option("inferSchema", "true").csv("Files/01_raw/orders.csv")
df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("overwrite").saveAsTable("raw_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# RED FROM RAW DELTA TABLE 
df_raw = spark.table("raw_orders")
df_raw.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, upper

df_clean = (
    df_raw
    # Cast order_date to DATE
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    
    # Ensure quantity > 0
    .filter(col("quantity") > 0)
    
    # Ensure unit_price > 0
    .filter(col("unit_price") > 0)
    
    # Standardize order_status to UPPER case
    .withColumn("order_status", upper(col("order_status")))
)

df_clean.printSchema()
df_clean.show(10)
df_clean.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("stg_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_stg = spark.table("stg_orders")

df_stg.show(10)
df_stg.printSchema()
df_stg.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stg.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

invalid_qty_count = df_stg.filter(df_stg.quantity <= 0).count()
invalid_qty_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stg.groupBy("order_status").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

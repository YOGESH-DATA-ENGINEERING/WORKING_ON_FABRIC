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

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit

df_stg = spark.table("stg_orders")

total_count = df_stg.count()
invalid_qty_count = df_stg.filter(df_stg.quantity <= 0).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

monitoring_df = spark.createDataFrame(
    [
        (
            "orders_pipeline",   # pipeline_name
            "silver",            # layer_name
            "stg_orders",        # table_name
            total_count,
            invalid_qty_count
        )
    ],
    [
        "pipeline_name",
        "layer_name",
        "table_name",
        "total_records",
        "invalid_quantity_count"
    ]
).withColumn("run_timestamp", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

monitoring_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("pipeline_monitoring")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("pipeline_monitoring").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stg = spark.table("stg_orders")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stg.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, year, month, when
df_stg = spark.table("stg_orders")

df_curated = (
    df_stg
    # Derive order year
    .withColumn("order_year", year(col("order_date")))
    
    # Derive order month
    .withColumn("order_month", month(col("order_date")))
    
    # Business bucket for order value
    .withColumn(
        "order_value_bucket",
        when(col("total_amount") < 5000, "LOW")
        .when((col("total_amount") >= 5000) & (col("total_amount") <= 15000), "MEDIUM")
        .otherwise("HIGH")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_curated.select(
    "order_date",
    "total_amount",
    "order_year",
    "order_month",
    "order_value_bucket"
).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_curated.groupBy("order_value_bucket").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_curated.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_orders")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim = spark.table("dim_orders")

df_dim.show(10)
df_dim.printSchema()
df_dim.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim.groupBy("order_value_bucket").count().alias("bucket_count").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim.groupBy("order_status").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import format_number, sum, col

df_dim = spark.table("dim_orders")

df_dim.groupBy("city") \
      .agg(
          format_number(sum(col("total_amount")), 2).alias("total_revenue")
      ) \
      .orderBy(col("total_revenue").desc()) \
      .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr

df_dim.groupBy("city") \
      .agg(
          expr("CAST(SUM(total_amount) AS DECIMAL(18,2))").alias("total_revenue")
      ) \
      .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, sum

df_dim = spark.table("dim_orders")

df_agg_city_sales = (
    df_dim
    .groupBy("city", "order_year")
    .agg(
        count("*").alias("total_orders"),
        sum(col("total_amount")).alias("total_revenue")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# (Recommended) Cast revenue to DECIMAL for finance
from pyspark.sql.functions import expr

df_agg_city_sales = df_agg_city_sales.withColumn(
    "total_revenue",
    expr("CAST(total_revenue AS DECIMAL(18,2))")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write as Delta table (Gold Serving)
df_agg_city_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("agg_city_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate the table
df_agg = spark.table("agg_city_sales")

df_agg.show(10)
df_agg.printSchema()
df_agg.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, sum, expr

df_dim = spark.table("dim_orders")

df_agg_monthly_sales = (
    df_dim
    .groupBy("order_year", "order_month")
    .agg(
        count("*").alias("total_orders"),
        sum(col("total_amount")).alias("total_revenue")
    )
)

df_agg_monthly_sales = df_agg_monthly_sales.withColumn(
    "total_revenue",
    expr("CAST(total_revenue AS DECIMAL(18,2))")
)

df_agg_monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("agg_monthly_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_agg = spark.table("agg_monthly_sales")

df_agg.show(10)
df_agg.printSchema()
df_agg.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Top 5 Cities by Revenue
from pyspark.sql.functions import col, sum

spark.table("agg_city_sales") \
     .groupBy("city") \
     .agg(
         sum(col("total_revenue")).alias("city_revenue")
     ) \
     .orderBy(col("city_revenue").desc()) \
     .limit(5) \
     .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

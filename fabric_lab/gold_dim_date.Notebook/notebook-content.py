# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "226c5c11-88b7-4451-b4a3-a8662628424e",
# META       "default_lakehouse_name": "Fabric_lab_LH",
# META       "default_lakehouse_workspace_id": "2463f80e-91a5-4a1f-b5bc-3a6fbc0cf5ff",
# META       "known_lakehouses": [
# META         {
# META           "id": "226c5c11-88b7-4451-b4a3-a8662628424e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************



from pyspark.sql import functions as F

SOURCE_TABLE = "silver.orders"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{TARGET_SCHEMA}.dim_date"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

orders_df = spark.table(SOURCE_TABLE)

dim_date_src = (
    orders_df
    .select(F.col("order_date").alias("date"))
    .dropDuplicates()
    .filter(F.col("date").isNotNull())
)

dim_date = (
    dim_date_src
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("year", F.year("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
    .select(
        "date_key",
        "date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "day_of_week",
        "week_of_year",
        "is_weekend"
    )
)

(
    dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {TARGET_TABLE}")
display(spark.table(TARGET_TABLE))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.orders"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{TARGET_SCHEMA}.dim_product"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

orders_df = spark.table(SOURCE_TABLE)

dim_product_src = (
    orders_df
    .select(
        F.trim(F.col("product_name")).alias("product_name")
    )
    .dropDuplicates()
    .filter(F.col("product_name").isNotNull())
)

product_window = Window.orderBy("product_name")

dim_product = (
    dim_product_src
    .withColumn("product_key", F.row_number().over(product_window))
    .select(
        "product_key",
        "product_name"
    )
)

(
    dim_product
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

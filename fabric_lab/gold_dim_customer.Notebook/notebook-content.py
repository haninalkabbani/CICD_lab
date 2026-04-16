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

# MARKDOWN ********************


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "silver.orders"
TARGET_SCHEMA = "gold"
TARGET_TABLE = f"{TARGET_SCHEMA}.dim_customer"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

orders_df = spark.table(SOURCE_TABLE)

dim_customer_src = (
    orders_df
    .select(
        F.trim(F.col("customer_name")).alias("customer_name"),
        F.lower(F.trim(F.col("customer_email"))).alias("customer_email")
    )
    .dropDuplicates()
    .filter(
        F.col("customer_name").isNotNull() |
        F.col("customer_email").isNotNull()
    )
)

customer_window = Window.orderBy("customer_name", "customer_email")

dim_customer = (
    dim_customer_src
    .withColumn("customer_key", F.row_number().over(customer_window))
    .select(
        "customer_key",
        "customer_name",
        "customer_email"
    )
)

(
    dim_customer
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

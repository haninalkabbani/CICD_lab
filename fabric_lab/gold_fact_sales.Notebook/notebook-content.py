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
DIM_CUSTOMER_TABLE = "gold.dim_customer"
DIM_PRODUCT_TABLE = "gold.dim_product"
DIM_DATE_TABLE = "gold.dim_date"
TARGET_TABLE = "gold.fact_sales"

orders_df = spark.table(SOURCE_TABLE)
dim_customer_df = spark.table(DIM_CUSTOMER_TABLE)
dim_product_df = spark.table(DIM_PRODUCT_TABLE)
dim_date_df = spark.table(DIM_DATE_TABLE)

fact_sales = (
    orders_df.alias("o")
    .join(
        dim_customer_df.alias("c"),
        on=[
            F.coalesce(F.col("o.customer_name"), F.lit("")) == F.coalesce(F.col("c.customer_name"), F.lit("")),
            F.coalesce(F.col("o.customer_email"), F.lit("")) == F.coalesce(F.col("c.customer_email"), F.lit(""))
        ],
        how="left"
    )
    .join(
        dim_product_df.alias("p"),
        on=F.col("o.product_name") == F.col("p.product_name"),
        how="left"
    )
    .join(
        dim_date_df.alias("d"),
        on=F.col("o.order_date") == F.col("d.date"),
        how="left"
    )
    .select(
        F.col("o.sales_order_number"),
        F.col("o.line_number"),
        F.col("c.customer_key"),
        F.col("p.product_key"),
        F.col("d.date_key"),
        F.col("o.order_date"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.tax_amount"),

        F.round(F.col("o.quantity") * F.col("o.unit_price"), 4).alias("sales_amount"),
        F.round(F.col("o.tax_amount"), 4).alias("tax_amount_line"),

        # preferred business formula
        F.round(
            (F.col("o.quantity") * F.col("o.unit_price")) + F.col("o.tax_amount"),
            4
        ).alias("total_amount"),

        F.col("o.etl_loaded_at")
    )
)

missing_dim_keys = fact_sales.filter(
    F.col("customer_key").isNull() |
    F.col("product_key").isNull() |
    F.col("date_key").isNull()
)

missing_count = missing_dim_keys.count()
print(f"Rows with missing dimension keys: {missing_count}")

(
    fact_sales
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

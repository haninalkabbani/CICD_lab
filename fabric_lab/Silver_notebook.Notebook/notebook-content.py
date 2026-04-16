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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Import Required Packages

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 1- Define needed Parameters

# CELL ********************

TARGET_SCHEMA = "silver"
TARGET_TABLE_NAME = "orders"
TARGET_TABLE = f"{TARGET_SCHEMA}.{TARGET_TABLE_NAME}"
SOURCE_PATH="Files/bronze/current/*.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#   # 2- Create schema if it does not exist

# CELL ********************

# --------------------------------------------
# 2) Create schemas if they do not exist
# --------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 3- Defined Bronze Schema

# CELL ********************

bronze_orders_schema = T.StructType([
    T.StructField("sales_order_number", T.StringType(), False),
    T.StructField("line_number", T.IntegerType(), False),
    T.StructField("order_date_raw", T.StringType(), True),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("customer_email", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DecimalType(18, 4), True),
    T.StructField("tax_amount", T.DecimalType(18, 4), True)])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# ## 4- Read from CSV File

# CELL ********************

# --------------------------------------------
# 3) Read file
# --------------------------------------------
raw_df = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("multiLine", "false")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .schema(bronze_orders_schema)
    .load(SOURCE_PATH)
)
print("Raw schema:")
raw_df.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # 5- Clean / standardize data

# CELL ********************

df = raw_df

# Trim string columns
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

# Empty strings -> null
for c in string_cols:
    df = df.withColumn(
        c,
        F.when(F.col(c) == "", F.lit(None)).otherwise(F.col(c))
    )

# Parse and standardize
clean_df = (
    df
    .withColumn("order_date", F.to_date("order_date_raw", "yyyy-MM-dd"))
    .withColumn("customer_email", F.lower(F.col("customer_email")))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,4)"))
    .withColumn("tax_amount", F.col("tax_amount").cast("decimal(18,4)"))
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 4))
    .withColumn("etl_loaded_at", F.current_timestamp())
    .drop("order_date_raw")
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 6- Create Target Table

# CELL ********************

if not spark.catalog.tableExists(TARGET_TABLE):
    (
        clean_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    print(f"Created target table: {TARGET_TABLE}")
else:
    print(f"Target table already exists: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 7- Upsert into Delta table

# CELL ********************


target = DeltaTable.forName(spark, TARGET_TABLE)

merge_condition = """
t.sales_order_number = s.sales_order_number
AND t.line_number = s.line_number
"""

update_set = {
    "order_date": "s.order_date",
    "customer_name": "s.customer_name",
    "customer_email": "s.customer_email",
    "product_name": "s.product_name",
    "quantity": "s.quantity",
    "unit_price": "s.unit_price",
    "tax_amount": "s.tax_amount",
    "gross_amount": "s.gross_amount",
    "etl_loaded_at": "s.etl_loaded_at"
}

insert_set = {
    "sales_order_number": "s.sales_order_number",
    "line_number": "s.line_number",
    "order_date": "s.order_date",
    "customer_name": "s.customer_name",
    "customer_email": "s.customer_email",
    "product_name": "s.product_name",
    "quantity": "s.quantity",
    "unit_price": "s.unit_price",
    "tax_amount": "s.tax_amount",
    "gross_amount": "s.gross_amount",
    "etl_loaded_at": "s.etl_loaded_at"
}

(
    target.alias("t")
    .merge(clean_df.alias("s"), merge_condition)
    .whenMatchedUpdate(set=update_set)
    .whenNotMatchedInsert(values=insert_set)
    .execute()
)

print(f"Upsert completed into {TARGET_TABLE}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# # 11- Final check


# CELL ********************


final_df = spark.table(TARGET_TABLE)
print("Final target count:", final_df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

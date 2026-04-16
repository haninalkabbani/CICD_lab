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
import zipfile
import os
import glob


zip_path = "/lakehouse/default/Files/Bronze_orders/orders.zip"

extract_path = "/lakehouse/default/Files/Bronze_orders/"

print("Zip file location:", zip_path)
print("Extracting to:", extract_path)

# Optional: remove old CSV files so reruns don't duplicate data
for f in glob.glob(extract_path + "*.csv"):
    os.remove(f)
    print("Removed old file:", f)

# Extract files
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print("Files extracted successfully!")

# Show extracted files
print("\nFiles now in Bronze_orders folder:")
for f in os.listdir(extract_path):
    print(f)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

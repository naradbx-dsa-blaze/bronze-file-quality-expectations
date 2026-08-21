# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: create landing volume and load sample payment files
# MAGIC
# MAGIC Creates the target schema + volume and writes three sample JSON files:
# MAGIC - `payments_2024_01.json` – 5 valid rows
# MAGIC - `payments_2024_02.json` – 5 rows, **2 with NULL `payment_date`** (the bad file)
# MAGIC - `payments_2024_03.json` – 5 valid rows
# MAGIC
# MAGIC Run this once before running the pipeline. Parameters are provided as
# MAGIC job parameters / notebook widgets so nothing is hard-coded.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "file_quality_demo")
dbutils.widgets.text("volume", "fq_demo_landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

base = f"/Volumes/{catalog}/{schema}/{volume}/payments"
dbutils.fs.mkdirs(base)
print(f"Landing path: {base}")

# COMMAND ----------

# JSON Lines (one record per line) is what Auto Loader's `json` format expects.
file_01 = """{"payment_id": "P0001", "vendor_id": "V100", "payment_date": "2024-01-03", "amount": 1250.00, "currency": "USD"}
{"payment_id": "P0002", "vendor_id": "V101", "payment_date": "2024-01-05", "amount": 875.50, "currency": "USD"}
{"payment_id": "P0003", "vendor_id": "V100", "payment_date": "2024-01-09", "amount": 4300.00, "currency": "USD"}
{"payment_id": "P0004", "vendor_id": "V102", "payment_date": "2024-01-11", "amount": 210.25, "currency": "USD"}
{"payment_id": "P0005", "vendor_id": "V101", "payment_date": "2024-01-15", "amount": 990.00, "currency": "USD"}"""

# File 2: rows P0007 and P0009 have a NULL payment_date -> the "problematic" file.
file_02 = """{"payment_id": "P0006", "vendor_id": "V100", "payment_date": "2024-02-02", "amount": 1500.00, "currency": "USD"}
{"payment_id": "P0007", "vendor_id": "V103", "payment_date": null, "amount": 640.00, "currency": "USD"}
{"payment_id": "P0008", "vendor_id": "V101", "payment_date": "2024-02-08", "amount": 3200.75, "currency": "USD"}
{"payment_id": "P0009", "vendor_id": "V102", "payment_date": null, "amount": 415.00, "currency": "USD"}
{"payment_id": "P0010", "vendor_id": "V100", "payment_date": "2024-02-14", "amount": 780.00, "currency": "USD"}"""

file_03 = """{"payment_id": "P0011", "vendor_id": "V101", "payment_date": "2024-03-01", "amount": 2100.00, "currency": "USD"}
{"payment_id": "P0012", "vendor_id": "V100", "payment_date": "2024-03-04", "amount": 560.25, "currency": "USD"}
{"payment_id": "P0013", "vendor_id": "V104", "payment_date": "2024-03-07", "amount": 8800.00, "currency": "USD"}
{"payment_id": "P0014", "vendor_id": "V102", "payment_date": "2024-03-12", "amount": 145.00, "currency": "USD"}
{"payment_id": "P0015", "vendor_id": "V103", "payment_date": "2024-03-19", "amount": 1020.50, "currency": "USD"}"""

dbutils.fs.put(f"{base}/payments_2024_01.json", file_01, overwrite=True)
dbutils.fs.put(f"{base}/payments_2024_02.json", file_02, overwrite=True)
dbutils.fs.put(f"{base}/payments_2024_03.json", file_03, overwrite=True)

# COMMAND ----------

display(dbutils.fs.ls(base))

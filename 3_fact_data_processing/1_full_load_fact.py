# Databricks notebook source
# let's import all the libraries that might be useful
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# let's get the widgets
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

# COMMAND ----------

# MAGIC %md
# MAGIC let's get access to the utilites

# COMMAND ----------

# MAGIC %run 
# MAGIC /Workspace/Users/ahsanhabibsunny85@gmail.com/combined_pipeline/1_setup/utilities
# MAGIC

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

# let's define the path (base path from where we get the data folder, landing path from where we get the exact files, and processed path where we keep the data files which is already ingested)

base_path = f"s3://sports-kingdom/{data_source}"
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)

# COMMAND ----------

# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# read the data from s3 bucket landing folder

df_raw = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv") \
                   .withColumn("read_timestamp", F.current_timestamp()) \
                   .withColumn("source_file", F.col("_metadata.file_name"))



# COMMAND ----------

# move the raw data into our bronze table

df_raw.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("append") \
            .saveAsTable(bronze_table)

# COMMAND ----------

# Moving files from source to processed directory
for file in dbutils.fs.ls(landing_path):
  dbutils.fs.mv(file.path, f"{processed_path}/{file.name}", True)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Silver Transformation**

# COMMAND ----------

# Read the data from bronze layer

df_orders = spark.table(bronze_table).drop("read_timestamp", "source_file")
display(df_orders.limit(5))

# COMMAND ----------

# let's check our order_id column (No nulls so let's just keep it as it is)
df_orders.filter(F.col("order_id").isNull())

# converting customer id as integer and dropping all the invalid customer id
# let's drop the invalid customer code
df_orders = df_orders.filter(F.col("customer_id").rlike("^[0-9]+$")) \
                     .withColumn("customer_code", F.col("customer_id").cast("integer")) \
                     .drop("customer_id")

display(df_orders.limit(5))

# COMMAND ----------

# let's transform the order_placement_date (dates are in different format, let's convert them into one standard format)
# 1. First, strip out day names (e.g., "Tuesday, ") using regexp_replace
df_orders = df_orders.withColumn("raw_date", F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", ""))

df_orders = df_orders.withColumn("order_date",
    F.coalesce(
        F.try_to_date(F.col("raw_date"), "yyyy-MM-dd"),     # 2024-01-01
        F.try_to_date(F.col("raw_date"), "dd/MM/yyyy"),     # 01/01/2024 (Day first)
        F.try_to_date(F.col("raw_date"), "MM/dd/yyyy"),     # 01/01/2024 (Month first)
        F.try_to_date(F.col("raw_date"), "yyyy/MM/dd"),     # 2024/01/01
        F.try_to_date(F.col("raw_date"), "dd-MM-yyyy"),     # 01-01-2024
        F.try_to_date(F.col("raw_date"), "MMM dd, yyyy"),   # Jan 01, 2024
        F.try_to_date(F.col("raw_date"), "MMMM dd, yyyy")   # January 01, 2024
    )
).drop("raw_date")

# COMMAND ----------

# drop duplciates

df_orders = df_orders.dropDuplicates(["order_id", "order_date", "customer_code", "product_id", "order_qty"])

# convert product_id as string

df_order = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

# let's check the data
df_orders = df_orders.drop("order_placement_date")
display(df_orders.limit(5))

# COMMAND ----------

# Join with the products table to get the product_code

df_products = spark.table("fmcg.silver.products")

df_orders = df_orders.join(df_products, ["product_id"], "inner").select(df_orders["*"], df_products["product_code"])
df_orders = df_orders.withColumnRenamed("customer_id", "customer_code")

display(df_orders.limit(5))


# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    df_orders.write.format("delta").options(**{"delta.enableChangeDataFeed": "true","mergeSchema": "true"}).mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark,silver_table)
    silver_delta.alias("silver") \
                .merge(df_orders.alias("bronze"), 
                       "silver.order_id = bronze.order_id and silver.order_date = bronze.order_date and silver.customer_id = bronze.customer_id and silver.product_id = bronze.product_id") \
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold**

# COMMAND ----------

df_gold = spark.table(silver_table)
display(df_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 20
if not spark.catalog.tableExists(gold_table):
    df_gold.write.format("delta").options(**{"delta.enableChangeDataFeed" : "true", "mergeSchema" : "true"}).mode("overwrite") \
        .saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark,gold_table) 
    gold_delta.alias("source").merge(df_gold.alias("gold"), 
                                "source.order_date = gold.order_date AND source.order_id = gold.order_id \
                                AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code") \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Merging with Parent Company**

# COMMAND ----------

# we can see our child table data are on day level for each customer (but in parent table it's month level, so we need to fix it)
df_child = spark.table(gold_table)
display(df_child.filter(F.col("customer_code") == "789320"))

# COMMAND ----------

# DBTITLE 1,Cell 22
df_child_monthly = df_child \
    .withColumn("date", F.trunc(F.col("order_date"), "month")) \
    .groupBy("date", "customer_code", "product_code") \
    .agg(F.sum("order_qty").alias("sold_quantity")) \
    .select("date", "product_code", "customer_code", "sold_quantity")

display(df_child_monthly.limit(5))
              

# COMMAND ----------

# DBTITLE 1,Convert fact_orders date column from STRING to DATE
# Convert the date column from STRING (MM/dd/yyyy) to proper DATE type in fact_orders table
parent_table = f"{catalog}.{gold_schema}.fact_orders"

if spark.catalog.tableExists(parent_table):
    print(f"Converting date column in {parent_table} from STRING to DATE...")
    
    # Read the parent table
    df_parent = spark.table(parent_table)
    
    # Convert the string date to proper DATE type
    df_parent_fixed = df_parent.withColumn("date", F.try_to_date(F.col("date"), "yyyy-MM-dd"))
    
    # Overwrite the table with the corrected schema
    df_parent_fixed.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(parent_table)
    
    print("✓ Date column successfully converted to DATE type")
    
    # Verify the schema
    print("\nUpdated schema:")
    spark.table(parent_table).printSchema()
else:
    print(f"Table {parent_table} does not exist yet. It will be created with correct DATE type during merge.")

# COMMAND ----------

# DBTITLE 1,Cell 24
# Merge with parent table (both now have DATE type)
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(
    df_child_monthly.alias("child_gold"), 
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
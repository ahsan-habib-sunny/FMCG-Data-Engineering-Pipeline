# Databricks notebook source
# import all the necessary librarires needed to carry out the task

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run 
# MAGIC /Workspace/Users/ahsanhabibsunny85@gmail.com/combined_pipeline/1_setup/utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog =  dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


# COMMAND ----------

# defining the path of data

base_path = f"s3://sports-kingdom/{data_source}"
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

print(f"Base Path: {base_path}")
print(f"Landing Path: {landing_path}")
print(f"Processed Path: {landing_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Bronze**

# COMMAND ----------

df_raw = spark.read.csv(f"{landing_path}*.csv", inferSchema = True, header = True) \
                   .withColumn("read_timestamp", F.current_timestamp()) \
                   .withColumn("source_file", F.col("_metadata.file_name"))

print(f"total count of this data file: {df_raw.count()}")
display(df_raw.limit(5))



# COMMAND ----------

# now we have to transfer this ingested raw data to our bronze layer, keeping in mind that we have to append the data with the old one
# if we overwrite, we will lose all the historical data

df_raw.write.format("delta") \
            .options(**{"mergeSchema": "true", "delta.enableChangeDataFeed" : "true"}) \
            .mode("append") \
            .saveAsTable(bronze_table)

# COMMAND ----------

### Staging table to process just the arrived incremenal data to do transformation in silver layer otherwise need to work with whole bronze data
df_raw.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# COMMAND ----------

# let's move the files to processed from landing path

for file in dbutils.fs.ls(landing_path):
    dbutils.fs.mv(file.path, f"{processed_path}{file.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Silver Transformation**

# COMMAND ----------

df_orders = spark.table(f"{catalog}.{bronze_schema}.staging_{data_source}").drop("read_timestamp", "source_file")
display(df_orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# let's check our order_id column (No nulls so let's just keep it as it is)
df_orders.filter(F.col("order_id").isNull())

# converting customer id as integer and dropping all the invalid customer id
# let's drop the invalid customer code
df_orders = df_orders.filter(F.col("customer_id").rlike("^[0-9]+$")) \
                     .withColumn("customer_code", F.col("customer_id").cast("integer")) \
                     .drop("customer_id")

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
        F.try_to_date(F.col("raw_date"), "MMMM dd, yyyy"),  # January 01, 2024
        F.try_to_date(F.col("raw_date"), "MMMM d, yyyy")   # January 1, 2024
    )
).drop("raw_date")

# drop duplciates

df_orders = df_orders.dropDuplicates(["order_id", "order_date", "customer_code", "product_id", "order_qty"])

# convert product_id as string

df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# let's check the data
df_orders = df_orders.drop("order_placement_date")
display(df_orders.limit(5))

# COMMAND ----------

# join with products table
df_products = spark.table(f"{catalog}.{silver_schema}.products")

df_orders_joined = df_orders.join(df_products, on = "product_id", how = "inner").select(df_orders["*"], df_products["product_code"])
print(f"total count of this data file: {df_orders_joined.count()}")
display(df_orders_joined.limit(5))


# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_orders_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_orders_joined.alias("bronze"), "silver.order_date = bronze.order_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_code = bronze.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# staging for incremental data (main reason of keeping the joined data here is if we need to come back and do some other transformational tasks on the incremental data we can use this staging table rather than using the original silver table which also contains historical data. another important thing is we need to keep the incremental silver data to gold layer as well.)

df_orders_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold Layer**

# COMMAND ----------

df_gold = spark.table(f"{catalog}.{silver_schema}.staging_{data_source}")

display(df_gold.limit(2))

# COMMAND ----------

df_gold.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Merging with child table**

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.order_date = gold.order_date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Merging with parent table**

# COMMAND ----------

df_sts_orders = spark.table(f"{catalog}.{silver_schema}.staging_{data_source}")

# this is in day level, convert it into month level to merge with parent table
display(df_sts_orders.filter(F.col("customer_code") == 789220)) 

# COMMAND ----------

df_sts_final_orders = df_sts_orders.groupBy("order_date","product_code","customer_code") \
             .agg(F.sum("order_qty").alias("sold_quantity"))

display(df_sts_final_orders.limit(5))

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_sts_final_orders.alias("child_gold"), "parent_gold.date = child_gold.order_date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdate(set = {"date": "child_gold.order_date", "product_code": "child_gold.product_code", "customer_code": "child_gold.customer_code", "sold_quantity": "child_gold.sold_quantity"}).whenNotMatchedInsert(values = {"date": "child_gold.order_date", "product_code": "child_gold.product_code", "customer_code": "child_gold.customer_code", "sold_quantity": "child_gold.sold_quantity"}).execute()

# COMMAND ----------

# we can just staging now if we want

staging_table_bronze = f"{catalog}.{bronze_schema}.staging_{data_source}"
staging_table_silver = f"{catalog}.{silver_schema}.staging_{data_source}"

# Drop the table from Unity Catalog / Hive Metastore
spark.sql(f"DROP TABLE IF EXISTS {staging_table_bronze}")
spark.sql(f"DROP TABLE IF EXISTS {staging_table_silver}")
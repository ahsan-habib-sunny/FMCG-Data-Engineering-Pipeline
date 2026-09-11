# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/ahsanhabibsunny85@gmail.com/combined_pipeline/1_setup/utilities
# MAGIC

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sports-kingdom/{data_source}/*.csv"
print(base_path)


# COMMAND ----------

# DBTITLE 1,Cell 5
# read the dataframe

df = spark.read.format("csv") \
               .option("header", "true") \
               .option("inferSchema", "true") \
               .load(base_path) \
               .withColumn("read_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.col("_metadata.file_name")) 
display(df.limit(5))

# COMMAND ----------

# write it to bronze layer

df.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Silver Processing**

# COMMAND ----------

# let's read the data from bronze layer

df_bronze = spark.read.table(f'{catalog}.{bronze_schema}.{data_source}')
display(df_bronze.limit(5))

# COMMAND ----------

# let's check if we have any duplicate customer information
display(df_bronze.groupBy(F.col("customer_id")).count().filter(F.col("count") > 1))

# COMMAND ----------

# drop the duplicates
df_silver = df_bronze.dropDuplicates(["customer_id"])

# let's check if we still have any duplicate customer information
display(df_silver.groupBy(F.col("customer_id")).count().filter(F.col("count") > 1))

# COMMAND ----------

# let's trim any leading or trailing spaces from customer name and city
df_silver = df_silver.withColumn("customer_name", F.trim(F.col("customer_name"))) \
                     .withColumn("city", F.trim(F.col("city")))

# Standardization
df_silver = df_silver.withColumn("customer_name", F.initcap(F.col("customer_name"))) \
                     .withColumn("city", F.initcap(F.col("city")))

# COMMAND ----------

# DBTITLE 1,Cell 12
# let's handle the city names annomalies

city_mapping = {'Bengaluruu' : 'Bengalore',
                 'Bengaluru' : 'Bengalore',
                 'Chenai' : 'Chennai',
                 'Channai' : 'Chennai',
                 'Lucknau' : 'Lucknow',
                 'Kolkatta' : 'Kolkata',
                 'Calcutta' : 'Kolkata',
                 'Jaypur' : 'Jaipur',
                 'Mumabi' : 'Mumbai',
                 'Mumbay' : 'Mumbai',
                 'Newdheli' : 'New Delhi',
                 'Newdelhi' : 'New Delhi',
                 'Newdelhee' : 'New Delhi',
                 'Poone' : 'Pune',
                 'Ahmadabad' : 'Ahmedabad',
                 'Ahemdabad' : 'Ahmedabad',
                 'Hyderabadd' : 'Hyderabad'}

allowed = ['Bengalore', 'Chennai', 'Lucknow', 'Kolkata', 'Jaipur', 'Mumbai', 'New Delhi', 'Pune', 'Ahmedabad', 'Hyderabad']

df_silver = df_silver.replace(city_mapping, subset=["city"]) \
                     .withColumn("city", F.when(F.col("city").isin(allowed),F.col("city"))
                                          .when(F.col("city").isNull(), None)
                                          .otherwise(None))
                     
display(df_silver.select("city").distinct())

# COMMAND ----------

# let's get some more column for the business need
df_silver = df_silver.withColumn(
                        "customer", F.concat(F.col("customer_name"), F.lit("-"), F.coalesce(F.col("city"),F.lit("Unknown")))) \
                     .withColumn("market", F.lit("India")) \
                     .withColumn("platform", F.lit("Sports Bar")) \
                     .withColumn("channel", F.lit("Acquisition"))

display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 14
# write the silver table into silver layer now
df_silver.write.format("delta") \
         .option("delta.enableChangeDataFeed", "true") \
         .option("mergeSchema", "true") \
         .mode("overwrite") \
         .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold Processing**
# MAGIC

# COMMAND ----------

df_silver = spark.read.table(f'{catalog}.{silver_schema}.{data_source}')

# only selecting those columns into gold layer that are needed for the busines
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")
display(df_gold.limit(5))

# COMMAND ----------

# write into gold schema

df_gold.write.format("delta") \
             .option("delta.enableChangeDataFeed", "true") \
             .mode("overwrite") \
             .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")


# COMMAND ----------

# now let's metge the sb_dim_customers (child company) to dim_customer (parent company)

df_parent_customers = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_{data_source}")
df_child_customers = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}") \
                          .select(F.col("customer_id").alias("customer_code"),
                                  "customer", "market", "platform", "channel")

df_parent_customers.alias("target") \
        .merge(df_child_customers.alias("source"), "target.customer_code = source.customer_code") \
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

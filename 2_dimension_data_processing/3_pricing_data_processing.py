# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %run 
# MAGIC /Workspace/Users/ahsanhabibsunny85@gmail.com/combined_pipeline/1_setup/utilities
# MAGIC

# COMMAND ----------

# let's get the widgets to use as UI
dbutils.widgets.text("catalog", "fmcg" , "Catalog")
dbutils.widgets.text("data_source", "gross_price" , "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sports-kingdom/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

# read the dataframe

df_raw = spark.read.format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(base_path) \
                   .withColumn("read_timestamp", F.current_timestamp()) \
                   .withColumn("source_file", F.col("_metadata.file_name")) 

# COMMAND ----------

# write it to bronze layer

df_raw.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("overwrite") \
            .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Silver Processing**

# COMMAND ----------

# read the data from bronze layer

df_bronze = spark.table(f"{catalog}.{bronze_schema}.{data_source}")
display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 9
# Pass each potential pattern to to_date inside coalesce
df_silver = df_bronze.withColumn("date", 
                        F.coalesce(
                            F.try_to_date(F.col("month"), "yyyy-MM-dd"),
                            F.try_to_date(F.col("month"), "dd-MM-yyyy"),
                            F.try_to_date(F.col("month"), "dd/MM/yyyy"),
                            F.try_to_date(F.col("month"), "MM/dd/yyyy"),
                            F.try_to_date(F.col("month"), "MMM dd, yyyy"),
                            F.try_to_date(F.col("month"), "yyyy/MM/dd"))) 

df_silver = df_silver.withColumn("month", F.date_format(F.col("date"), "MMM")) \
                     .withColumn("year", F.date_format(F.col("date"), "yyyy"))

display(df_silver.limit(5))

# COMMAND ----------

print(f"total count before dropping duplicates: {df_silver.count()}")
# let's drop the duplicates
df_silver = df_silver.dropDuplicates(["product_id", "date", "gross_price"])
print(f"total count after dropping duplicates: {df_silver.count()}")

# COMMAND ----------

# DBTITLE 1,Cell 10
# Handling gross price
# We are validating the gross_price column, converting only valid numeric values to double, fixing negative prices by making them positive, and replacing all non-numeric values with 0

# First filter to only valid numeric strings, cast to double or replace with 0
df_silver = df_silver.withColumn("gross_price_num", \
                        F.when(F.col("gross_price").rlike(r'^\d+$'), F.col("gross_price").cast("double")) \
                        .otherwise(F.lit(0.0)))

# Then handle positive/negative values
df_silver = df_silver.withColumn("gross_price", \
                        F.when(F.col("gross_price_num") > 0, F.col("gross_price_num")) \
                        .when(F.col("gross_price_num") < 0, F.abs(F.col("gross_price_num"))) \
                        .otherwise(F.lit(0.0))) \
                     .drop("gross_price_num")

display(df_silver.limit(5))

# COMMAND ----------

# We enrich the silver dataset by performing an inner join with the products table to fetch the correct product_code for each product_id.

df_product = spark.table("fmcg.silver.products")

df_joined = df_silver.alias("silver") \
    .join(
        df_product.alias("product").select("product_id", "product_code"), 
        on="product_id", 
        how="inner"
    ) \
    .select(
        "product_id", 
        F.col("product.product_code").alias("product_code"),  # Explicitly select from 'product' table
        F.col("silver.date").alias("date"),
        "month", 
        "year", 
        "gross_price", 
        "read_timestamp", 
        "source_file"
    )

df_joined.show()


# COMMAND ----------

# DBTITLE 1,Cell 12
# let's get rid of duplicates

df_joined = df_joined.dropDuplicates(["product_code", "year", "month"])


# COMMAND ----------

df_joined.write \
         .format("delta") \
         .option("delta.enablechangeDataFeed", 'true') \
         .option("mergeSchema", "true") \
         .mode("overwrite") \
         .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold**

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.{data_source}")
display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 15
df_gold = df_silver.select("product_code", "date", "year", "month", "gross_price")

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("overwriteSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")



# COMMAND ----------

# Merging data source with parent data source
# but first we need to aggregate the child data source by year for each product

window_spec = Window.partitionBy("product_code","year").orderBy(F.col("is_zero"), F.col("date").desc())

df_gold = df_gold.withColumn("is_zero", F.when(F.col("gross_price") == 0, 1). otherwise(0)) \
                 .withColumn("rank", F.rank().over(window_spec)) \
                 .filter(F.col("rank") == 1) \
                 .drop("date", "month", "is_zero", "rank")

display(df_gold.filter(F.col("product_code") == "0b065309d5a4d557321dfd1bfef4eed4a3613830330204ff17ca0bd57f7ff300")) 




# COMMAND ----------

## Take required cols

df_gold = df_gold.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")

# change year to string
df_gold = df_gold.withColumn("year", F.col("year").cast("string"))

# df_gold = df_gold.dropDuplicates(["product_code", "price_inr" , "year"])

display(df_gold.limit(5))

# COMMAND ----------

from pyspark.sql.window import Window

# Keep only the latest year's price per product_code before merging
window_spec = Window.partitionBy("product_code").orderBy(F.col("year").desc())

df_gold_dedup = df_gold.withColumn("rn", F.row_number().over(window_spec)) \
                       .filter(F.col("rn") == 1) \
                       .drop("rn")

# Now merge with the deduplicated source
parent_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")

parent_table.alias("target").merge(
    source=df_gold_dedup.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()

# COMMAND ----------


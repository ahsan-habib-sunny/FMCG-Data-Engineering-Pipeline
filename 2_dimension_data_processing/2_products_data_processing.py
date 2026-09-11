# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %run 
# MAGIC /Workspace/Users/ahsanhabibsunny85@gmail.com/combined_pipeline/1_setup/utilities
# MAGIC

# COMMAND ----------

print(f"name of bronze schema: {bronze_schema},\
        name of silver schema: {silver_schema},\
        name of gold schema: {gold_schema}")

# COMMAND ----------

# let's get the widgets to use as UI
dbutils.widgets.text("catalog", "fmcg" , "Catalog")
dbutils.widgets.text("data_source", "products" , "Data Source")

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

display(df_raw.limit(5))

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

# let's get rid of all the leading and trailing spaces and also standardization
df_silver = df_bronze.withColumn("product_name", F.initcap(F.trim(F.col("product_name")))) \
                    .withColumn("product_id", F.trim(F.col("product_id"))) \
                    .withColumn("category", F.initcap(F.trim(F.col("category"))))

display(df_silver.limit(5))

# COMMAND ----------

# Drop duplicates

df_silver = df_silver.dropDuplicates(["product_id"])



# COMMAND ----------

# DBTITLE 1,Cell 12
# let's get the product variant separate from the product name
df_silver = df_silver.withColumn("variant",F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)) \
                     .withColumn("product_name", F.trim(F.split(F.col("product_name"),('\\(')).getItem(0)))
display(df_silver.limit(5))

# COMMAND ----------

# handling the null in the category by replacing it with the value for same product
from pyspark.sql.window import Window
window_spec = Window.partitionBy("product_name").orderBy("product_id")

df_silver = df_silver.withColumn("category", F.when(F.col("category").isNull(), F.first("category").over(window_spec))
                                             .otherwise(F.col("category")))

display(df_silver.select('*').filter(F.col("product_name") == 'Sportsbar Energy Bar Mixed Berry').filter(F.col("variant") == '40g'))

# COMMAND ----------

# Replace 'protien' → 'protein' in both product_name and category
df_silver = df_silver.withColumn("product_name", F.regexp_replace(F.col("product_name"),"(?i)Protien", "Protein")) \
                     .withColumn("category", F.regexp_replace(F.col("category"),"(?i)Protien", "Protein"))


# COMMAND ----------

# Standardizing Customer Attributes to Match Parent Company Data Model
## 1: Add division column
df_silver = df_silver.withColumn("division",
                    F.when(F.col("category") == "Energy Bars", "Nutrition Bars") \
                    .when(F.col("category") == "Protein Bars", "Nutrition Bars") \
                    .when(F.col("category") == "Granola & Cereals", "Breakfast Foods") \
                    .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery") \
                    .when(F.col("category") == "Healthy Snacks", "Healthy Snacks") \
                    .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes") 
                    .otherwise("Other"))

## 2: Create new column: product_code  
# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver = df_silver.withColumn("product_code",
                                 F.sha2(F.col("product_name").cast("string"),256)) \
                     .withColumn("product_id",
                                 F.when(F.col("product_id").rlike(r"^\d+$"),F.col("product_id"))
                                  .otherwise(F.lit("99999"))) \
                     .withColumnRenamed("product_name", "product")



# COMMAND ----------

df_silver = df_silver.select("product_code", "division", "category", "product", \
                             "variant", "product_id", "read_timestamp", "source_file")

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC # **Gold Layer**

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.{data_source}")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
display(df_gold.limit(5))

# COMMAND ----------

df_gold.write\
       .format("delta") \
       .option("delta.enableChangeDataFeed", "true") \
       .mode("overwrite") \
       .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# Merging data with parent data

df_parent_products = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.dim_{data_source}")
df_child_products = spark.table(f"{catalog}.{gold_schema}.sb_dim_{data_source}") \
                         .select("product_code", "division", "category", "product", "variant") \
                         .dropDuplicates(["product_code"])

df_parent_products.alias("target") \
                  .merge(df_child_products.alias("source"),"target.product_code = source.product_code") \
                  .whenMatchedUpdate(set = {"division" : "source.division", "category" : "source.category", \
                                            "product" : "source.product", "variant" : "source.variant"}) \
                  .whenNotMatchedInsert(values = {"product_code" : "source.product_code", "division" : "source.division", \
                                               "category" : "source.category", "product" : "source.product", \
                                               "variant" : "source.variant"}) \
                  .execute()


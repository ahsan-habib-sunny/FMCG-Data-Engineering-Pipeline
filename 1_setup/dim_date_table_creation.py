# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Defining start date and end date
start_date = "2024-01-01" 
end_date = "2025-12-01"

# COMMAND ----------

# Generate one row per month starting from start date till we get row for end date
df_input = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])

df = df_input.withColumn(
                    "month_date",
                    F.explode(F.sequence(F.to_date(F.col("start_date")), F.to_date(F.col("end_date")), F.expr("interval 1 month")))) \
                    .withColumn("date_key", F.date_format(F.col("month_date"), "yyyyMM")) \
                    .withColumn("year", F.year(F.col("month_date"))) \
                    .withColumn("month_name", F.date_format(F.col("month_date"), "MMMM")) \
                    .withColumn("month_short_name", F.date_format(F.col("month_date"), "MMM")) \
                    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter(F.col("month_date")))) \
                    .withColumn("year_quarter", F.concat(F.year(F.col("month_date")), F.lit("-Q"), F.quarter(F.col("month_date")))) \
                    .select("month_date", "date_key", "year", "month_name", "month_short_name", "quarter", "year_quarter")

display(df)

# COMMAND ----------

# save as a table

df.write.mode("overwrite").format("delta").saveAsTable("fmcg.gold.dim_date")
# Databricks notebook source
df_jan = spark.read.parquet("dbfs:/FileStore/tables/yellow_tripdata_2025_01.parquet")
df_feb = spark.read.parquet("dbfs:/FileStore/tables/yellow_tripdata_2025_02.parquet")


# COMMAND ----------

df_jan.printSchema()
df_feb.printSchema()

# COMMAND ----------

display(df_jan.count())
display(df_feb.count())

# COMMAND ----------

df_jan.show(5)
df_feb.show(5)

# COMMAND ----------

df = df_jan.unionByName(df_feb)

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, to_date, sum, window, max

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Revenue Column

# COMMAND ----------

df = df.withColumn("Revenue", col("fare_amount") + col("extra") + col("mta_tax") + col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") + col("total_amount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Passengers by Area

# COMMAND ----------

df.groupBy("PULocationID").sum("passenger_count").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real-time Avg Fare / Total Amount by Vendor

# COMMAND ----------

df.groupBy("VendorID").agg(avg("fare_amount").alias("avg_fare"), avg("total_amount").alias("avg_total_earning")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving Count of Payments by Mode

# COMMAND ----------

from pyspark.sql.window import Window

window_spec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime").rowsBetween(-10, 0)
df.withColumn("moving_payment_count", count("payment_type").over(window_spec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 2 Vendors by Revenue on a Given Date

# COMMAND ----------

date_filtered = df.withColumn("date", to_date("tpep_pickup_datetime"))
(date_filtered.groupBy("date", "VendorID")
    .agg(
        sum("Revenue").alias("total_revenue"),
        sum("passenger_count").alias("total_passenger"),
        sum("trip_distance").alias("total_distance")
    )
    .orderBy("total_revenue", ascending=False)
    .show(2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Most Passengers Between Two Locations

# COMMAND ----------

df.groupBy("PULocationID", "DOLocationID").sum("passenger_count").orderBy("sum(passenger_count)", ascending=False).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top Pickup Locations in Last Day

# COMMAND ----------

max_timestamp = df.select(max("tpep_pickup_datetime")).first()[0]

# COMMAND ----------

from pyspark.sql.types import TimestampType
from datetime import timedelta
cutoff_timestamp = max_timestamp - timedelta(days=1)
last_day_df = df.filter(col("tpep_pickup_datetime") >= cutoff_timestamp)

# COMMAND ----------

last_day_df.groupBy("PULocationID").sum("passenger_count").orderBy("sum(passenger_count)", ascending=False).show()

# COMMAND ----------


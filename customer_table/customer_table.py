# Databricks notebook source
# MAGIC %run "/marketing_tech/COMMON LIB/Read Google Sheet"

# COMMAND ----------

today = dbutils.widgets.get("today")

# COMMAND ----------

# GET S3 DATA POINT
last_paid_channel = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/csv_keboola/lpc/" + today + ".data.csv")
promotion = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/csv_keboola/promo/" + today + ".data.csv")
new_customer = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/csv_keboola/new_customer/" + today + ".data.csv") 
probably_from_tv = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/csv_keboola/tv/" + today + ".data.csv")  
fare_usd = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/csv_keboola/fare_usd/" + today + ".data.csv")

# GET DATA FROM GOOGLE SHEET (CHANNEL MAPPING)
channel_mapping = get_gsheet_toSparkDF(os.path.join("/dbfs/mnt/S3_mktg_prod_general/mktg/bima_playground/", "drive_api.json"), "11diCtDiYPB7nJhXoz5QEKjjLlfhOKAYHbfERkFiZkRQ", "Sheet1")

# COMMAND ----------

# WRITE DATA TO PARQUET (TEMPORARY FILES)
last_paid_channel.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/last_paid_channel")
promotion.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/promotion")
new_customer.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/new_customer")
probably_from_tv.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/probably_from_tv")
fare_usd.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/fare_usd")
channel_mapping.write.format("parquet").mode("overwrite").save("/mnt/S3_mktg_prod_data/v1/intermediary/mta-143_channel_performance/activity/parquet/channel_mapping")
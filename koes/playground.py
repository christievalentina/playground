# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/S3_qbp/philip_thomas/customer_master/2017/10/10/21/00/

# COMMAND ----------

cost_fi = sqlContext.read.format('csv').options(header='true',inferSchema='true').load('/mnt/koes_playground/MTA144/prediction_results/feature_impact/cost_feature_impact_hotel_ID.csv');
cost_fi.createOrReplaceTempView("cost_fi");
ca_fi = sqlContext.read.format('csv').options(header='true',inferSchema='true').load('/mnt/koes_playground/MTA144/prediction_results/feature_impact/ca_feature_impact_hotel_ID.csv');
ca_fi.createOrReplaceTempView("ca_fi");

# COMMAND ----------

fi_df = spark.sql("select *,'cost' as type from cost_fi union select *,'ca' as type from cost_fi order by type,_c0");display(fi_df);

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/mnt/koes_playground/MTA144/prediction_results/feature_impact/

# COMMAND ----------



# COMMAND ----------

jsonDf = spark.read.format("json").json("dbfs:/mnt/arditto_playground/drive_api/dssgsheet-9abfa3a64f79.json")
display(jsonDf)

# COMMAND ----------

# MAGIC %sh less /dbfs/databricks/init/Data_Science_Cluster/script01-R-upgrade.sh

# COMMAND ----------

# MAGIC %sh less /dbfs/databricks/init/Data_Science_Cluster/script02-R-install-pkg.sh

# COMMAND ----------

print("haha")
par1 = dbutils.widgets.get("param_country")
par2 = dbutils.widgets.get("param_product")
print(par1 + ' - ' + par2)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/

# COMMAND ----------

blacklisted_email = spark.sql("""
  select 'ahooi99@gmail.com' as email
  union select 'ainy_fauziyah@yahoo.com' as email
  union select 'alex_otoole@hotmail.com' as email
  union select 'alilee7@hotmail.com' as email
  union select 'alsay88@gmail.com' as email
  union select 'andimedia.owner@gmail.com' as email
  union select 'b.lanaucbt@gmail.com' as email
  union select 'barryreid@outlook.com' as email
  union select 'beatricejc29@gmail.com' as email
  union select 'ckpejman@yahoo.com' as email
  union select 'cutiesil@hotmail.com' as email
  union select 'dennis.mangilet@ge.com' as email
  union select 'dharen008@gmail.com' as email
  union select 'elnovinmaharsyah@ymail.com' as email
  union select 'expressions_by_luli@yahoo.com' as email
  union select 'fernandodioni@binusian.org' as email
  union select 'glazedfoil@gmail.com' as email
  union select 'gregory_pim@yahoo.com' as email
  union select 'h.srituti@ranchmarket.co.id' as email
  union select 'id@celax.asia' as email
  union select 'info@patrickrichard.com' as email
  union select 'karyakamibersama@yahoo.com' as email
  union select 'letoan89@gmail.com' as email
  union select 'marc.de.smet@outlook.com' as email
  union select 'mhood747@gmail.com' as email
  union select 'milena.sahm@tu-dortmund.de' as email
  union select 'nata_adit@live.com' as email
  union select 'nathan@techsure.ca' as email
  union select 'nattida.chn@gmail.com' as email
  union select 'nilen@online.no' as email
  union select 'noah.f@hotmail.com.au' as email
  union select 'perdiansyahs11@gmail.com' as email
  union select 'renhart.yonathan@gmail.com' as email
  union select 'rickywsiek@hotmail.com' as email
  union select 'rizqi_mestica@yahoo.com' as email
  union select 'robertc@truemail.co.th' as email
  union select 'ryanps@gmail.com' as email
  union select 'sirjman@gmail.com' as email
  union select 'sutatno.sudarga@platinumceramics.com' as email
  union select 'teoferrari77@gmail.com' as email
  union select 'thirdwd@gmail.com' as email
  union select 'tshany89@gmail.com' as email
  union select 'unogusdin@rocketmail.com' as email
  union select 'uriel.pinsonneault@gmail.com' as email
  union select 'veravandorp@hotmail.com' as email
  union select 'vieramelia@gmail.com' as email
  union select 'zipporahantonio@gmail.com' as email
""")
blacklisted_email.createOrReplaceTempView("blacklisted_email")

# COMMAND ----------

import datetime
def get_date_range(start_date, end_date):
    d1 = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    d2 = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    diff = d2 - d1
    date_range = "*{"
    for i in range(diff.days + 1):
        date_range = date_range + (d1 + datetime.timedelta(i)).strftime('%Y/%m/%d') + "/*/*/*,"
    date_range = date_range[:len(date_range)-1] +  "}" # "}.json"
    return date_range

# COMMAND ----------

import datetime
def get_date_range_avro(start_date, end_date):
    d1 = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    d2 = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    diff = d2 - d1
    date_range = "*{"
    for i in range(diff.days + 1):
        date_range = date_range + (d1 + datetime.timedelta(i)).strftime('%Y/%m/%d') + "/*/*,"
    date_range = date_range[:len(date_range)-1] +  "}" # "}.json"
    return date_range

# COMMAND ----------

query_date = get_date_range('2017-01-01','2017-07-30')
file_name = "dbfs:/mnt/S3_denorm_hour_1/track.flight.issued/"
file_path = file_name + query_date
flight_issued = spark.read.json(file_path)
flight_issued.createOrReplaceTempView("flight_issued")

# COMMAND ----------

print(type(flight_issued))

# COMMAND ----------

flight_issued_count = spark.sql(""" 
  select count(distinct contactEmail)
  from flight_issued
  where
    country = 'TH'
    and contactEmail not in (select * from blacklisted_email)
""")
display(flight_issued_count)

# COMMAND ----------

bl_count = spark.sql("""
  select count(*)
  from blacklisted_email
""")
display(bl_count)

# COMMAND ----------

flight_issued_cleaned = spark.sql(""" 
  select distinct contactEmail
  from flight_issued
  where
    country = 'TH'
    and contactEmail not in (select * from blacklisted_email)
    and contactEmail != ''
""")
flight_issued_cleaned_count = spark.sql(""" 
  select count(distinct contactEmail)
  from flight_issued
  where
    country = 'TH'
    and contactEmail not in (select * from blacklisted_email)
    and contactEmail != ''
""")
flight_example = spark.sql("""
  select distinct contactEmail
  from flight_issued
  where
    country = 'TH'
    and contactEmail not in (select * from blacklisted_email)
    and contactEmail != ''
  limit 10
""")

# COMMAND ----------

print(type(flight_issued_cleaned))

# COMMAND ----------

display(flight_issued_cleaned_count)

# COMMAND ----------

display(flight_issued_cleaned)

# COMMAND ----------

import hashlib
flight_issued_hashed = flight_issued_cleaned.rdd.map(lambda row: hashlib.sha256(str(row)).hexdigest())
print(type(flight_issued_hashed))

# COMMAND ----------

flight_issued_hashed.collect()

# COMMAND ----------

flight_issued_hashed.count()

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

# schema = StructType([StructField(str(row), StringType(), True) for row in range(32)])
# hashed_email_df = sqlContext.createDataFrame(flight_issued_hashed, schema)
hashed_email_df = flight_issued_hashed.toDF(StructType([StructField("hashed_email", StringType(), True)]))
type(hashed_email_df)

# COMMAND ----------

print flight_issued_hashed

# COMMAND ----------

from pyspark.sql import Row

row = Row("hashed_email") # Or some other column name
display(flight_issued_hashed.map(row).toDF())

# hashed_email_df = sqlContext.createDataFrame(flight_issued_hashed, ["tes"])

# COMMAND ----------

# from datetime import datetime
import datetime
import re
import ast
import json
import numpy as np

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import LongType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import MapType
from pyspark.sql.types import BooleanType

datetime.datetime.today().date()

# COMMAND ----------

ts_trx_result = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/mnt/koes_playground/MTA144/prediction_results/ts_trx/ts_trx_pred_result_flight_TH.csv')

# COMMAND ----------

ts_trx_result.createOrReplaceTempView("ts_trx_result")

# COMMAND ----------

ts_trx_result_df = spark.sql("""
select 
  cast(date as date) as dt,
  case 
    when num_trx_pred = 'inf' then cast(0 as float)
    else cast(num_trx_pred as float)
  end as num_trx_pred,
  case 
    when num_trx_pred_lower = 'inf' then cast(0 as float)
    else cast(num_trx_pred_lower as float)
  end as num_trx_pred_lower,
  case 
    when num_trx_pred_upper = 'inf' then cast(0 as float)
    else cast(num_trx_pred_upper as float)
  end as num_trx_pred_upper
  -- case 
  --   when num_trx_pred_base = 'inf' then cast(0 as float)
  --   else cast(num_trx_pred_base as float)
  -- end as num_trx_pred_base
from ts_trx_result
""")
display(ts_trx_result_df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/S3_mktg_prod_general/mktg/koes_playground/

# COMMAND ----------

df_hotel_issued = spark.read.parquet("dbfs:/mnt/S3_mktg_prod_data/v1/temp/adhoc_eoy_trend/df_hotel_issued")
df_hotel_issued.createOrReplaceTempView("df_hotel_issued")

df_search_hotel_final = spark.read.parquet("dbfs:/mnt/S3_mktg_prod_data/v1/temp/adhoc_eoy_trend/df_search_hotel_final")
df_search_hotel_final .createOrReplaceTempView("df_search_hotel_final ")

df_flight_issued = spark.read.parquet("dbfs:/mnt/S3_mktg_prod_data/v1/temp/adhoc_eoy_trend/df_flight_issued")
df_flight_issued.createOrReplaceTempView("df_flight_issued")

# COMMAND ----------

hotel_issued = spark.sql("""
select *
from df_hotel_issued
where status != 'BOOKED'
limit 10
""")
display(hotel_issued)

# COMMAND ----------

hotel_search = spark.sql("""
select *
from df_search_hotel_final
limit 10
""")
display(hotel_search)

# COMMAND ----------

flight_issued = spark.sql("""
select *
from df_flight_issued
limit 10
""")
display(flight_issued)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/S3_mktg_prod_data/v1/denorm/parquet/flight/day_1_wib/issued_with_channel_attribution/issued_date_wib=2017-02-27/

# COMMAND ----------

# READ DATA TO PARQUET
issued_w_channel_attribution = spark.read.parquet("/mnt/S3_mktg_prod_data/v1/denorm/parquet/flight/day_1_wib/issued_with_channel_attribution/issued_date_wib=2017-03-15/")

# CREATING ALIASES FOR FUTURE QUERIES
issued_w_channel_attribution.createOrReplaceTempView("issued_w_channel_attribution")

# COMMAND ----------

ch_attr = spark.sql("""
  select *
  from issued_w_channel_attribution
  where weight_joint_promo > 0
""")

display(ch_attr)
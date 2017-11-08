# Databricks notebook source
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import json

# COMMAND ----------

#READ FROM PARQUET
df_dim_bank = spark.read.format("parquet").load("mnt/S3_mktg_prod_data/v1/final/parquet/dimension/rawjs.bank")
df_dim_bank_iin = spark.read.format("parquet").load("mnt/S3_mktg_prod_data/v1/final/parquet/dimension/rawjs.bank.iin")
df_user_invoice = spark.read.format("parquet").load("mnt/S3_mktg_prod_data/v1/raw/parquet/payment/rawjs.payment_commerce_user_invoice")
df_payment_request = spark.read.format("parquet").load("mnt/S3_mktg_prod_data/v1/raw/parquet/payment/rawjs.payment.payment.request")

df_dim_bank.createOrReplaceTempView("dim_bank")
df_dim_bank_iin.createOrReplaceTempView("dim_bank_iin")
df_user_invoice.createOrReplaceTempView("user_invoice")
df_payment_request.createOrReplaceTempView("payment_request")

# COMMAND ----------

sqlContext.sql("set spark.sql.caseSensitive=true")

# COMMAND ----------

#CHANGE ID TYPE ON BANK
dim_bank_cleaned = spark.sql("""
  SELECT    
    _id AS id,
    countryCode AS country_code,
    name,
    shortName AS short_name
  FROM
    dim_bank
""")
dim_bank_cleaned.createOrReplaceTempView("dim_bank_cleaned")

# COMMAND ----------

display(dim_bank_cleaned)

# COMMAND ----------

#CHANGE ID AND BANKISSUERID TYPEON B
dim_bank_iin_cleaned = spark.sql("""
 SELECT
    _id AS id,
    bankIssuerId AS bank_issuer_id,
    cardProcessor AS card_processor,
    cardSeries AS card_series,
    cardType AS card_type,
    countryCode as country_code
  FROM
    dim_bank_iin
""")
dim_bank_iin_cleaned.createOrReplaceTempView("dim_bank_iin_cleaned")

# COMMAND ----------

display(dim_bank_iin_cleaned)

# COMMAND ----------

#JOIN DIM BANK AND DIM BANK IIN 
dim_bank_iin_joined = spark.sql("""
  SELECT  bank_iin.id AS iin_id,
          bank_iin.bank_issuer_id AS bank_issuer_id,
          bank_iin.country_code AS iin_country_code,
          bank.country_code AS bank_country_code,
          bank.name AS bank_name,
          bank.short_name AS bank_short_name,
          bank_iin.card_processor AS card_processor,
          bank_iin.card_series AS card_series,
          bank_iin.card_type AS card_type
  FROM dim_bank_iin_cleaned AS bank_iin
  LEFT JOIN dim_bank_cleaned AS bank
  ON bank_iin.bank_issuer_id = bank.id
""")
dim_bank_iin_joined.createOrReplaceTempView("dim_bank_iin_joined")

# COMMAND ----------

display(dim_bank_iin_joined)

# COMMAND ----------

#ADD RANK FOR EACH _id BASED ON LAST UPDATE TIME
user_invoice_ranked = spark.sql("""
  SELECT
    user_invoice.*,
    rank() OVER (PARTITION BY _id ORDER BY __lut DESC) AS rank
  FROM
    ( SELECT *
      FROM user_invoice
    ) AS user_invoice
""")
user_invoice_ranked.createOrReplaceTempView("user_invoice_ranked")

# COMMAND ----------

display(user_invoice_ranked)

# COMMAND ----------

#CHOOSE RANK=1 TO GET THE MOST UPDATED DATA  
user_invoice_most_updated = spark.sql("""
  SELECT
    _id AS invoice_id,
    invoiceCreationTime AS invoice_creation_time,
    __lut AS lut,
    profileId AS profile_id,
    userId AS user_id,
    orderEntries
  FROM
    user_invoice_ranked
  WHERE rank = 1
""")

# COMMAND ----------

display(user_invoice_most_updated)

# COMMAND ----------

#PARSE ORDER ENTRIES
#CREATE UDF TO ACCESS DF
def coupon_detail (input):
  order_entries_json = input
  booking_id = ''
  product_name = ''
  product_currency = ''
  product_amount = ''
  unique_code_currency = ''
  unique_code_amount = ''
  coupon_code = ''
  coupon_currency = ''
  coupon_amount = ''
  coupon_qty = ''
  installment_code = ''
  installment_currency = ''
  installment_amount = ''
  point_currency = ''
  point_amount = ''
  insurance_currency = ''
  insurance_amount = ''

  booking_id = order_entries_json[0]['itemId']['id']
  
  for i in range(len(order_entries_json)):
    try : 
      if 'booking' in order_entries_json[i]['name'].lower() or 'transaction' in order_entries_json[i]['name'].lower() : 
        if('flight & booking' in order_entries_json[i]['name'].lower()) : product_name = 'Package'
        elif('flight' in order_entries_json[i]['name'].lower()) : product_name = 'Flight'
        elif('hotel' in order_entries_json[i]['name'].lower()) : product_name = 'Hotel'
        elif('train' in order_entries_json[i]['name'].lower()) : product_name = 'Train' 
        elif('experience' in order_entries_json[i]['name'].lower()) : product_name = 'Experience'
        elif('domestic' in order_entries_json[i]['name'].lower()) : product_name = 'Connectivity' 
        elif('roaming' in order_entries_json[i]['name'].lower()) : product_name = 'Connectivity' 
        elif('prepaidsim' in order_entries_json[i]['name'].lower()) : product_name = 'Connectivity'
        elif('wifirental' in order_entries_json[i]['name'].lower()) : product_name = 'Connectivity'
        else : product_name = 'Undefined'

      if (order_entries_json[i]['name'].split(' ')[-1]).lower() in ['booking', 'transaction']:  
          product_currency = order_entries_json[i]['amount']['currency']
          if product_currency in ['IDR','VND']:
            product_amount = order_entries_json[i]['amount']['amount']
          else:
            product_amount = order_entries_json[i]['amount']['amount']/100.00
            
      if((order_entries_json[i]['itemId']['kind']).lower() == 'unique_code'):
          unique_code_currency = order_entries_json[i]['amount']['currency']
          if unique_code_currency in ['IDR','VND']:
            unique_code_amount = order_entries_json[i]['amount']['amount'] 
          else:
            unique_code_amount = order_entries_json[i]['amount']['amount']/100.00
        
      if((order_entries_json[i]['itemId']['kind']).lower() == 'voucher'):
          if(coupon_code == ''):
            coupon_code = order_entries_json[i]['itemId']['scope']
            coupon_currency = order_entries_json[i]['amount']['currency']
            coupon_amount = str(order_entries_json[i]['amount']['amount'] * -1)
            coupon_qty = str(order_entries_json[i]['qty'])
          else:
            coupon_code += ';' + order_entries_json[i]['itemId']['scope']
            coupon_currency +=  ';' + order_entries_json[i]['amount']['currency']
            coupon_amount += ';' + str(order_entries_json[i]['amount']['amount'] * -1)
            coupon_qty +=  ';' + str(order_entries_json[i]['qty'])
            
      if((order_entries_json[i]['itemId']['kind']).lower() == 'installment_request'):
          installment_code = order_entries_json[i]['itemId']['scope']
          installment_currency = order_entries_json[i]['amount']['currency']
          if installment_currency in ['IDR','VND']:
            installment_amount = order_entries_json[i]['amount']['amount'] 
          else:
            installment_amount = order_entries_json[i]['amount']['amount']/100.00
            
      if((order_entries_json[i]['itemId']['kind']).lower() == 'point_redemption'):
          point_currency = order_entries_json[i]['amount']['currency']
          if point_currency in ['IDR','VND']:
            point_amount = order_entries_json[i]['amount']['amount'] 
          else:
            point_amount = order_entries_json[i]['amount']['amount']/100.00 
            
      if((order_entries_json[i]['itemId']['kind']).lower() == 'insurance_sales'):
          insurance_currency = order_entries_json[i]['amount']['currency']
          if insurance_currency in ['IDR','VND']:
            insurance_amount = order_entries_json[i]['amount']['amount'] 
          else:
            insurance_amount = order_entries_json[i]['amount']['amount']/100.00 
    except : 
      True
      
   
  output = []
  output.append(booking_id)
  output.append(product_name)
  output.append(product_currency)
  output.append(product_amount)
  output.append(unique_code_currency)
  output.append(unique_code_amount)
  output.append(coupon_code)
  output.append(coupon_currency)
  output.append(coupon_amount)
  output.append(coupon_qty)
  output.append(installment_code)
  output.append(installment_currency)
  output.append(installment_amount)
  output.append(point_currency)
  output.append(point_amount)
  output.append(insurance_currency)
  output.append(insurance_amount)
  return output

get_coupon_detail = udf(coupon_detail,StringType())

# COMMAND ----------

#CALL UDF
user_invoice_parsed =  user_invoice_most_updated.withColumn("coupon_detail", get_coupon_detail(user_invoice_most_updated.orderEntries))
user_invoice_parsed.createOrReplaceTempView('user_invoice_parsed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coupon_detail
# MAGIC FROM user_invoice_parsed
# MAGIC WHERE coupon_detail NOT LIKE '%,%'

# COMMAND ----------

#SELECT ATTRIBUTES
user_invoice_cleaned = spark.sql("""
  SELECT 
    invoice_id,
    invoice_creation_time,
    lut,
    profile_id,
    user_id,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[0] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[0] END booking_id,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[1] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[1] END product_name,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[2] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[2] END product_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[3] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[3] END product_amount,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[4] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[4] END unique_code_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[5] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[5] END unique_code_amount,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[6] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[6] END coupon_code,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[7] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[7] END coupon_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[8] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[8] END coupon_amount, 
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[9] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[9] END coupon_qty,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[10] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[10] END installment_code,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[11] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[11] END installment_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[12] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[12] END installment_amount,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[13] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[13] END point_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[14] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[14] END point_amount,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[15] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[15] END insurance_currency,
    CASE WHEN split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[16] = '' THEN ''
      ELSE split(translate(translate(coupon_detail , '[]','' ),', ',','), ',')[16] END insurance_amount
  FROM user_invoice_parsed
""")
user_invoice_cleaned.createOrReplaceTempView("user_invoice_cleaned")

# COMMAND ----------

display(user_invoice_parsed)

# COMMAND ----------

display(user_invoice_cleaned)

# COMMAND ----------

#CREATE UDF TO ACCESS DF
def coupon_name_detail (input):
  order_entries_json = input
  coupon_name = ''
  coupon_kind = ''

  for i in range(len(order_entries_json)):
          if(coupon_name == ''):
            coupon_name = str(order_entries_json[i]['name'])
          else:
            coupon_name += ';' + str(order_entries_json[i]['name'])
            
          if(coupon_kind == ''):
            coupon_kind = str(order_entries_json[i]['itemId']['kind'])
          else:
            coupon_kind += ';' + str(order_entries_json[i]['itemId']['kind'])
            
          
  output = []
  output.append(coupon_name)
  output.append(coupon_kind)
  return output
#   return coupon_name
get_coupon_name= udf(coupon_name_detail,StringType())

# COMMAND ----------

temp =  user_invoice_most_updated.withColumn("coupon_name", get_coupon_name(user_invoice_most_updated.orderEntries))
temp.createOrReplaceTempView('temp')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT DISTINCT
# MAGIC --     invoice_id,
# MAGIC     orderEntries,
# MAGIC    -- coupon_name,
# MAGIC     CASE WHEN split( translate( coupon_name , '[]','' ) , ',' )[0] = '' THEN ''
# MAGIC       ELSE split( translate( coupon_name , '[]','' ) , ',' )[0] END coupon_name,
# MAGIC     CASE WHEN split( translate( coupon_name , '[]','' ) , ',' )[1] = '' THEN ''
# MAGIC       ELSE split( translate( coupon_name , '[]','' ) , ',' )[1] END coupon_kind,
# MAGIC     paymentData
# MAGIC   FROM temp
# MAGIC )
# MAGIC WHERE LOWER(split( translate( coupon_name , '[]','' ) , ',' )[0]) LIKE '%voucher%' 
# MAGIC ----LOWER(coupon_name) NOT LIKE '%booking%' AND LOWER(coupon_name) NOT LIKE '%transaction%'
# MAGIC     --AND coupon_kind LIKE '%unique_code%'
# MAGIC   

# COMMAND ----------

#GET INVOICE ID WITH LATEST LUT ON PAYMENT REQUEST
payment_request_paid = spark.sql("""
  SELECT *
  FROM payment_request
  WHERE status = 'PAID'
""")

# COMMAND ----------

#CREATE UDF TO ACCESS DF
def invoice_id_detail (input):
  return input['id']

def tokens_detail (input):
  return input['card_no']

get_invoice_id_detail = udf(invoice_id_detail,StringType())
get_tokens_detail = udf(tokens_detail,StringType())

# COMMAND ----------

#CALL UDF
payment_request_invoice_id_parsed =  payment_request_paid.withColumn("invoice_id", get_invoice_id_detail(payment_request_paid.invoiceId))
payment_request_tokens_parsed =  payment_request_invoice_id_parsed.withColumn("card_no", get_tokens_detail(payment_request_invoice_id_parsed.tokens))
payment_request_tokens_parsed.createOrReplaceTempView('payment_request_tokens_parsed')

# COMMAND ----------

#SELECT ATTRIBUTES
payment_request_cleaned = spark.sql("""
  SELECT
    invoice_id,
    remarks AS booking_id,
    paymentMethod AS payment_request_method,
    card_no,
    --CASE paymentMethod
      --WHEN 'CREDIT_CARD' THEN card_no
      --ELSE ''
    --END AS card_number,
    savedPaymentMethodInvoke AS saved_payment_method_invoke
  FROM
    payment_request_tokens_parsed
""")
payment_request_cleaned.createOrReplaceTempView("payment_request_cleaned")

# COMMAND ----------

display(payment_request_cleaned)

# COMMAND ----------

payment_cleaned = spark.sql("""
SELECT ui.invoice_id,
        FROM_UNIXTIME(CAST(ui.invoice_creation_time AS BIGINT)/1000 + (7 * 3600)) AS invoice_creation_time_wib,
        FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600)) AS lut_wib,
        ui.profile_id,
        ui.user_id,
        ui.booking_id,
        ui.product_name,
        ui.product_currency,
        ui.product_amount,
        ui.unique_code_currency,
        ui.unique_code_amount,
        ui.coupon_code,
        ui.coupon_currency,
        ui.coupon_amount,
        ui.coupon_qty,
        ui.installment_code,
        ui.installment_currency,
        ui.installment_amount,
        ui.point_currency,
        ui.point_amount,
        ui.insurance_currency,
        ui.insurance_amount,
        pr.payment_request_method,
        pr.card_no,
        pr.saved_payment_method_invoke,
        dbi.bank_issuer_id,
        dbi.iin_country_code,
        dbi.bank_country_code,
        dbi.bank_name,
        dbi.bank_short_name,
        dbi.card_processor,
        dbi.card_series,
        dbi.card_type
FROM user_invoice_cleaned AS ui
LEFT JOIN payment_request_cleaned as pr
  ON ui.invoice_id = pr.invoice_id
  AND ui.booking_id = pr.booking_id
LEFT JOIN dim_bank_iin_joined AS dbi
  ON pr.card_no = dbi.iin_id
WHERE TO_DATE(FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600))) > TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)-(INTERVAL 3 day) AND TO_DATE(FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600))) <= TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)
ORDER BY 2 DESC
""")
payment_cleaned.createOrReplaceTempView("payment_cleaned")

# COMMAND ----------

payment_cleaned_temp = spark.sql("""
SELECT ui.invoice_id,
        FROM_UNIXTIME(CAST(ui.invoice_creation_time AS BIGINT)/1000 + (7 * 3600)) AS invoice_creation_time_wib,
        FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600)) AS lut_wib,
        ui.profile_id,
        ui.user_id,
        ui.booking_id,
        ui.product_name,
        ui.product_currency,
        ui.product_amount,
        ui.unique_code_currency,
        ui.unique_code_amount,
        ui.coupon_code,
        ui.coupon_currency,
        ui.coupon_amount,
        ui.coupon_qty,
        ui.installment_code,
        ui.installment_currency,
        ui.installment_amount,
        ui.point_currency,
        ui.point_amount,
        ui.insurance_currency,
        ui.insurance_amount,
        pr.payment_request_method,
        pr.card_no,
        pr.saved_payment_method_invoke,
        dbi.bank_issuer_id,
        dbi.iin_country_code,
        dbi.bank_country_code,
        dbi.bank_name,
        dbi.bank_short_name,
        dbi.card_processor,
        dbi.card_series,
        dbi.card_type
FROM user_invoice_cleaned AS ui
LEFT JOIN payment_request_cleaned as pr
  ON ui.invoice_id = pr.invoice_id
  AND ui.booking_id = pr.booking_id
LEFT JOIN dim_bank_iin_joined AS dbi
  ON pr.card_no = dbi.iin_id
WHERE TO_DATE(FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600))) > TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)-(INTERVAL 3 day) AND TO_DATE(FROM_UNIXTIME(CAST(ui.lut AS BIGINT)/1000 + (7 * 3600))) <= TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)
ORDER BY 2 DESC
""")
payment_cleaned_temp.createOrReplaceTempView("payment_cleaned_temp")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT (current_timestamp()+interval 7 hour)-interval 3 day

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from payment_cleaned
# MAGIC where booking_id = '223450752' OR booking_id ='222482102' OR booking_id ='222797688' OR booking_id ='222333708'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM (
# MAGIC   SELECT DISTINCT invoice_id
# MAGIC   FROM payment_cleaned
# MAGIC   WHERE lut_wib < TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR))
# MAGIC -- SELECT COUNT(*)
# MAGIC -- FROM payment_cleaned

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lut_wib
# MAGIC FROM payment_cleaned
# MAGIC WHERE lut_wib < TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

#WRITE TO PARQUET 
#......PARTITION......
payment_cleaned.write.mode('overwrite').parquet("/mnt/S3_mktg_prod_data/v1/final/parquet/payment/day_1_wib/payment_enrich/"+)

# COMMAND ----------

#GET DATA FROM JSON
payment_accumulated = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-150-etl_migrated_from_keboola_to_spark/payment_accumulated.csv")
raw_user_invoice = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-150-etl_migrated_from_keboola_to_spark/raw_commerce_user_invoice.csv")
raw_payment_request = spark.read.format("csv").option("header", "true").load("/mnt/S3_mktg_prod_data/v1/intermediary/mta-150-etl_migrated_from_keboola_to_spark/raw_payment_request.csv")

payment_accumulated.createOrReplaceTempView("payment_accumulated")
raw_user_invoice.createOrReplaceTempView("raw_user_invoice")
raw_payment_request.createOrReplaceTempView("raw_payment_request")

# COMMAND ----------

# MAGIC %sql
# MAGIC (select distinct invoiceId
# MAGIC   from raw_payment_request
# MAGIC   where status ='PAID')
# MAGIC   except
# MAGIC (select distinct invoice_id
# MAGIC   from payment_request_cleaned)
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT FROM_UNIXTIME(CAST(invoice_timestamp AS BIGINT)/1000 + (7 * 3600)) AS invoice_timestamp_wib 
# MAGIC --invoice_timestamp
# MAGIC FROM payment_accumulated
# MAGIC ORDER BY 1 ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM(
# MAGIC   SELECT DISTINCT invoice_id
# MAGIC   FROM payment_accumulated)
# MAGIC    --WHERE FROM_UNIXTIME(CAST(invoice_timestamp AS BIGINT)/1000 + (7 * 3600)) > (CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)-(INTERVAL 3 day))
# MAGIC -- SELECT COUNT(*)
# MAGIC -- FROM payment_accumulated

# COMMAND ----------

temp = spark.sql("""
SELECT a.invoice_id, FROM_UNIXTIME(CAST(b.invoice_timestamp AS BIGINT)/1000) AS time, b.booking_id FROM 
  (SELECT invoice_id FROM 
  (
  (SELECT DISTINCT invoice_id
    FROM payment_accumulated
  EXCEPT
  (SELECT DISTINCT invoice_id
    FROM payment_cleaned
    WHERE lut_wib < TO_DATE(CURRENT_TIMESTAMP()+INTERVAL 7 HOUR)))
  ))  AS a
  LEFT JOIN payment_accumulated AS b
  ON a.invoice_id= b.invoice_id 
  ORDER BY 2 DESC
  """)
temp.createOrReplaceTempView("temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select invoice_timestamp
# MAGIC from payment_accumulated
# MAGIC order by 1 desc

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*)
# MAGIC from payment_cleaned_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from payment_cleaned_temp
# MAGIC where invoice_id in (select invoice_id from temp)
# MAGIC order by 1 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select invoice_id, lut_wib
# MAGIC from payment_cleaned_temp
# MAGIC where invoice_id in (select invoice_id from temp)
# MAGIC order by 1 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select invoice_id, time
# MAGIC from temp
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC  SELECT a.invoice_id, FROM_UNIXTIME(CAST(b.invoice_timestamp AS BIGINT)/1000) AS time, b.booking_id FROM 
# MAGIC   (SELECT invoice_id FROM 
# MAGIC   (
# MAGIC   (SELECT DISTINCT invoice_id
# MAGIC     FROM payment_accumulated)
# MAGIC   EXCEPT
# MAGIC   (SELECT DISTINCT invoice_id
# MAGIC     FROM payment_cleaned_temp
# MAGIC     )
# MAGIC   )) AS a
# MAGIC   LEFT JOIN payment_accumulated AS b
# MAGIC   ON a.invoice_id= b.invoice_id 
# MAGIC   ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), 'acc' as name from payment_accumulated
# MAGIC union select count(*), 'clean' as name from payment_cleaned

# COMMAND ----------


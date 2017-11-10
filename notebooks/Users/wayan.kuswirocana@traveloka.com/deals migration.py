# Databricks notebook source
# MAGIC %fs
# MAGIC ls mnt/S3_mktg_prod_general/mktg/koes_playground/

# COMMAND ----------

# MAGIC %md ## get hotel deals requirement from s3

# COMMAND ----------

deals_req_loc = 'dbfs:/mnt/S3_mktg_prod_general/mktg/koes_playground/hotel_deals_req.csv'
deals_req = spark.read.format("com.databricks.spark.csv").option("header",True).load(deals_req_loc)

# COMMAND ----------

deals_req_pandas = deals_req.toPandas()
type(deals_req_pandas)

# COMMAND ----------

display(deals_req)

# COMMAND ----------

deals_req.registerTempTable("deals_req")
deals_req_tot_row = sql("""
  select count(*)
  from deals_req
""")
display(deals_req_tot_row)

# COMMAND ----------

# MAGIC %md ## get enriched hotel issued data from s3

# COMMAND ----------

hotel_issued_loc = 'dbfs:/mnt/S3_mktg_prod_general/mktg/koes_playground/latest_raw_hotel_issued_with_lp.csv'
hotel_issued = spark.read.format("com.databricks.spark.csv").option("header",True).load(hotel_issued_loc)

# COMMAND ----------

type(hotel_issued)

# COMMAND ----------

hotel_issued_pandas = hotel_issued.toPandas()
type(hotel_issued_pandas)

# COMMAND ----------

print(hotel_issued_pandas)

# COMMAND ----------

hotel_issued.registerTempTable("hotel_issued")
hotel_issued_tot_row = sql("""
  select count(*)
  from hotel_issued
""")
display(hotel_issued_tot_row)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## hotel deals processing

# COMMAND ----------

import csv
import datetime
import re
import time

from datetime import date
from datetime import datetime

def check_equal(req, value):
    if req == '*':
        return True
    elif value != 'undefined':
        if value == req:
            return True
        else:
            return False
    else:
        return False

def check_equal_gt(req, value):
    if req == '*':
        return True
    elif value != 'undefined':
        if value >= req:
            return True
        else:
            return False
    else:
        return False

csvlt = '\n'
csvdel = ','
csvquo = '"'

for row_joinres in hotel_issued_pandas.itertuples():
  visit_deals_promo_page = False
  split_flag = False
  use_deals_flag = False
  landing_page_flag = 'asdflkjhg'

  if row_joinres.booking_id != '':
      booking_id = row_joinres.booking_id
  else:
      booking_id = 'undefined'

  if row_joinres.session_id != '':
      session_id = row_joinres.session_id
  else:
      session_id = 'undefined'

  if row_joinres.issued_time != '':
      issued_timestamp = row_joinres.issued_time
      issued_temp = time.mktime(time.strptime(issued_timestamp, '%Y-%m-%d %H:%M:%S')) + 25200
      issued_date_format = time.strftime('%Y-%m-%d', time.localtime(issued_temp))
  else:
      issued_timestamp = 'undefined'
      issued_date_format = 'undefined'

  if row_joinres.booking_time != '':
      booking_timestamp = row_joinres.booking_time
      booking_temp = time.mktime(time.strptime(booking_timestamp, '%Y-%m-%d %H:%M:%S')) + 25200
      booking_date_format = time.strftime('%Y-%m-%d', time.localtime(booking_temp))
  else:
      booking_timestamp = 'undefined'
      booking_date_format = 'undefined'

  if row_joinres.check_in_date != '':
      #check_in_timestamp = row_joinres.check_in_date
      #check_in_temp = time.mktime(time.strptime(check_in_timestamp, '%d-%m-%Y'))
      #check_in_date = time.strftime('%Y-%m-%d', time.localtime(check_in_temp))
      check_in_date = row_joinres.check_in_date
  else:
      check_in_date = 'undefined'

  if row_joinres.check_out_date != '':
      #check_out_timestamp = row_joinres.check_out_date
      #check_out_temp = time.mktime(time.strptime(check_out_timestamp, '%d-%m-%Y'))
      #check_out_date = time.strftime('%Y-%m-%d', time.localtime(check_out_temp))
      check_out_date = row_joinres.check_out_date
  else:
      check_out_date = 'undefined'

  if issued_date_format != 'undefined' and check_in_date != 'undefined':
      issued_parse_temp = datetime.strptime(issued_date_format, '%Y-%m-%d')
      check_in_parse_temp = datetime.strptime(check_in_date, '%Y-%m-%d')
      date_to_check_in = int(abs((issued_parse_temp - check_in_parse_temp).days))
  else:
      date_to_check_in = 'undefined'

  if row_joinres.num_of_nights != '':
      num_of_nights = int(row_joinres.num_of_nights)
  else:
      num_of_nights = 'undefined'

  if row_joinres.num_rooms != '':
      num_rooms = int(row_joinres.num_rooms)
  else:
      num_rooms = 'undefined'

  if num_of_nights != 'undefined' and num_rooms != 'undefined':
      room_x_night = int(num_of_nights * num_rooms)
  else:
      room_x_night = 'undefined'

  if row_joinres.interface != '':
      intf = row_joinres.interface
  else:
      intf = 'undefined'

  if row_joinres.app_version != '':
      application_version = row_joinres.app_version
      application_version = application_version.replace(',','.')
      application_version = application_version.replace('.','')
      application_version = int(application_version)
      while(application_version < 100000):
          application_version *= 10
  else:
      application_version = 'undefined'

  if row_joinres.language_id != '':
      lang = row_joinres.language_id
  else:
      lang = 'undefined'

  if row_joinres.country_id != '':
      country = row_joinres.country_id
  else:
      country = 'undefined'

  if row_joinres.locale_id != '':
      locale = row_joinres.locale_id
  elif lang != 'undefined' and country != 'undefined':
      locale = lang + "_" + country
  else:
      locale = 'undefined'

  if row_joinres.hotel_id != '':
      hotel_id = row_joinres.hotel_id
  else:
      hotel_id = 'undefined'

  if row_joinres.brand_id != '':
      hotel_brand = row_joinres.brand_id
  else:
      hotel_brand = 'undefined'

  if row_joinres.chain_id != '':
      hotel_chain = row_joinres.chain_id
  else:
      hotel_chain = 'undefined'

  if row_joinres.area_name != '':
      hotel_area = row_joinres.area_name
  else:
      hotel_area = 'undefined'

  if row_joinres.city_name != '':
      hotel_city = row_joinres.city_name
  else:
      hotel_city = 'undefined'

  if row_joinres.region_name != '':
      hotel_region = row_joinres.region_name
  else:
      hotel_region = 'undefined'

  if row_joinres.country_name != '':
      hotel_country = row_joinres.country_name
  else:
      hotel_country = 'undefined'

  if row_joinres.lp_name != '':
      landing_page = row_joinres.lp_name
  else:
      landing_page = 'undefined'

  if row_joinres.is_new_customer != '':
      is_new_customer = row_joinres.is_new_customer.upper()
  else:
      is_new_customer = 'FALSE'

  if row_joinres.coupon_code != '':
      coupon_code = row_joinres.coupon_code
  else:
      coupon_code = 'undefined'

  if row_joinres.installment_code != '':
      installment_code = row_joinres.installment_code
  else:
      installment_code = 'undefined'

  for row_hotelreq in deals_req_pandas.itertuples():
      req_page_name = row_hotelreq.page_name
      req_routes_name = row_hotelreq.routes

      tempdate_start = row_hotelreq.start_promo_date
      if tempdate_start != '*':
          if '-' in tempdate_start:
              # tempdate_start = tempdate_start.split('-')
              start_date_temp = time.mktime(time.strptime(tempdate_start, '%Y-%m-%d'))
              req_start_promo_date = time.strftime('%Y-%m-%d', time.localtime(start_date_temp))
          if '/' in tempdate_start:
              # tempdate_start = tempdate_start.split('/')
              start_date_temp = time.mktime(time.strptime(tempdate_start, '%Y/%m/%d'))
              req_start_promo_date = time.strftime('%Y-%m-%d', time.localtime(start_date_temp))

          # date_start = int(tempdate_start[2])
          # month_start = int(tempdate_start[1])
          # year_start = int(tempdate_start[0])
          # req_start_promo_date = datetime.date(year_start, month_start, date_start)

      tempdate_end = row_hotelreq.end_promo_date
      if tempdate_end != '*':
          if '-' in tempdate_end:
              # tempdate_end = tempdate_end.split('-')
              end_date_temp = time.mktime(time.strptime(tempdate_end, '%Y-%m-%d'))
              req_end_promo_date = time.strftime('%Y-%m-%d', time.localtime(end_date_temp))
          if '/' in tempdate_end:
              # tempdate_end = tempdate_end.split('/')
              end_date_temp = time.mktime(time.strptime(tempdate_end, '%Y/%m/%d'))
              req_end_promo_date = time.strftime('%Y-%m-%d', time.localtime(end_date_temp))

          # date_end = int(tempdate_end[2])
          # month_end = int(tempdate_end[1])
          # year_end = int(tempdate_end[0])
          # req_end_promo_date = datetime.date(year_end, month_end, date_end)

      if row_hotelreq.hotel_id != '':
          req_hotel_id = row_hotelreq.hotel_id
      else:
          req_hotel_id = '*'

      if row_hotelreq.hotel_brand != '':
          req_hotel_brand = row_hotelreq.hotel_brand
      else:
          req_hotel_brand = '*'

      if row_hotelreq.hotel_chain != '':
          req_hotel_chain = row_hotelreq.hotel_chain
      else:
          req_hotel_chain = '*'

      if row_hotelreq.hotel_area != '':
          req_hotel_area = row_hotelreq.hotel_area
      else:
          req_hotel_area = '*'

      if row_hotelreq.hotel_city != '':
          req_hotel_city = row_hotelreq.hotel_city
      else:
          req_hotel_city = '*'

      if row_hotelreq.hotel_region != '':
          req_hotel_region = row_hotelreq.hotel_region
      else:
          req_hotel_region = '*'

      if row_hotelreq.hotel_country != '':
          req_hotel_country = row_hotelreq.hotel_country
      else:
          req_hotel_country = '*'

      tempdate_checkin = row_hotelreq.check_in_date
      if tempdate_checkin != '*':
          if '-' in tempdate_checkin:
              # tempdate_checkin = tempdate_checkin.split('-')
              checkin_temp = time.mktime(time.strptime(tempdate_checkin, '%Y-%m-%d'))
              req_check_in_date = time.strftime('%Y-%m-%d', time.localtime(checkin_temp))
          if '/' in tempdate_checkin:
              # tempdate_checkin = tempdate_checkin.split('/')
              checkin_temp = time.mktime(time.strptime(tempdate_checkin, '%Y/%m/%d'))
              req_check_in_date = time.strftime('%Y-%m-%d', time.localtime(checkin_temp))
          
          # date_checkin = int(tempdate_checkin[2])
          # month_checkin = int(tempdate_checkin[1])
          # year_checkin = int(tempdate_checkin[0])
          # req_check_in_date = datetime.date(year_checkin, month_checkin, date_checkin)

      tempdate_checkout = row_hotelreq.check_out_date
      if tempdate_checkout != '*':
          if '-' in tempdate_checkout:
              # tempdate_checkout = tempdate_checkout.split('-')
              checkout_temp = time.mktime(time.strptime(tempdate_checkout, '%Y-%m-%d'))
              req_check_out_date = time.strftime('%Y-%m-%d', time.localtime(checkout_temp))
          if '/' in tempdate_checkout:
              # tempdate_checkout = tempdate_checkout.split('/')
              checkout_temp = time.mktime(time.strptime(tempdate_checkout, '%Y/%m/%d'))
              req_check_out_date = time.strftime('%Y-%m-%d', time.localtime(checkout_temp))

          # date_checkout = int(tempdate_checkout[2])
          # month_checkout = int(tempdate_checkout[1])
          # year_checkout = int(tempdate_checkout[0])
          # req_check_out_date = datetime.date(year_checkout, month_checkout, date_checkout)

      if row_hotelreq.date_to_check_in != '':
          req_date_to_check_in = row_hotelreq.date_to_check_in
      else:
          req_date_to_check_in = '*'

      if row_hotelreq.num_of_room != '':
          req_num_of_room = row_hotelreq.num_of_room
      else:
          req_num_of_room = '*'

      if row_hotelreq.num_of_night != '':
          req_num_of_night = row_hotelreq.num_of_night
      else:
          req_num_of_night = '*'

      if row_hotelreq.room_x_night != '':
          req_room_x_night = row_hotelreq.room_x_night
      else:
          req_room_x_night = '*'

      req_locale = row_hotelreq.locale
      req_locale_split = req_locale.split('|')

      req_intf = row_hotelreq.intf
      req_intf_split = req_intf.split('|')

      req_app_version = row_hotelreq.app_version
      if req_app_version != '*':
          req_app_version = req_app_version.replace(',','.')
          req_app_version = req_app_version.replace('.','')
          req_app_version = int(req_app_version)
          while(req_app_version < 100000):
              req_app_version *= 10

      # iterate - check equal locale
      for i in range(0,len(req_locale_split)):
          if req_locale_split[i] == '*':
              equal_locale = True
              break
          elif locale == req_locale_split[i]:
              equal_locale = True
              break
          else:
              equal_locale = False

      # check equal landing page
      if req_start_promo_date == '*' and req_end_promo_date == '*':
          visit_between_promo = True
      elif issued_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              visit_between_promo = req_start_promo_date <= issued_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              visit_between_promo = req_start_promo_date <= issued_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              visit_between_promo = issued_date_format <= req_end_promo_date
          else:
              visit_between_promo = False
      elif booking_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              visit_between_promo = req_start_promo_date <= booking_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              visit_between_promo = req_start_promo_date <= booking_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              visit_between_promo = booking_date_format <= req_end_promo_date
          else:
              visit_between_promo = False
      else:
          visit_between_promo = False
      
      cleaned_req_page_name = re.sub('[^0-9a-zA-Z]+', '', req_page_name.lower())
      cleaned_req_routes_name = re.sub('[^0-9a-zA-Z]+', '', req_routes_name.lower())
      cleaned_landing_page = re.sub('[^0-9a-zA-Z]+', '', landing_page.lower())
      # visit_deals_promo_page = req_landing_page_name.lower() in landing_page.lower()
      if ((cleaned_req_page_name in cleaned_landing_page) or (cleaned_req_routes_name in cleaned_landing_page)) and visit_between_promo and equal_locale:
          visit_deals_promo_page = True
          landing_page = req_routes_name + '|' + req_page_name

      # check booking between promo date
      if req_start_promo_date == '*' and req_end_promo_date == '*':
          between_promo_date = True
      elif booking_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              between_promo_date = req_start_promo_date <= booking_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              between_promo_date = req_start_promo_date <= booking_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              between_promo_date = booking_date_format <= req_end_promo_date
          else:
              between_promo_date = False
      else:
          between_promo_date = False

      # check equal hotel id
      equal_hotel_id = check_equal(req_hotel_id, hotel_id)

      # check equal hotel brand
      equal_hotel_brand = check_equal(req_hotel_brand, hotel_brand)

      # check equal hotel chain
      equal_hotel_chain = check_equal(req_hotel_chain, hotel_chain)

      # check equal hotel area
      equal_hotel_area = check_equal(req_hotel_area, hotel_area)

      # check equal hotel city
      equal_hotel_city = check_equal(req_hotel_city, hotel_city)

      # check equal hotel region
      equal_hotel_region = check_equal(req_hotel_region, hotel_region)

      # check equal hotel country
      equal_hotel_country = check_equal(req_hotel_country, hotel_country)

      # check check in date between check in and check out date
      if req_check_in_date == '*' and req_check_out_date == '*':
          in_between_checkinout_date = True
      elif check_in_date != 'undefined':
          if req_check_in_date != '*' and req_check_out_date != '*':
              in_between_checkinout_date = req_check_in_date <= check_in_date <= req_check_out_date
          elif req_check_in_date != '*' and req_check_out_date == '*':
              in_between_checkinout_date = req_check_in_date <= check_in_date
          elif req_check_in_date == '*' and req_end_promo_date != '*':
              in_between_checkinout_date = check_in_date <= req_check_out_date
          else:
              in_between_checkinout_date = False
      else:
          in_between_checkinout_date = False

      # check check out date between check in and check out date
      if req_check_in_date == '*' and req_check_out_date == '*':
          out_between_checkinout_date = True
      elif check_out_date != 'undefined':
          if req_check_in_date != '*' and req_check_out_date != '*':
              out_between_checkinout_date = req_check_in_date <= check_out_date <= req_check_out_date
          elif req_check_in_date != '*' and req_check_out_date == '*':
              out_between_checkinout_date = req_check_in_date <= check_out_date
          elif req_check_in_date == '*' and req_end_promo_date != '*':
              out_between_checkinout_date = check_out_date <= req_check_out_date
          else:
              out_between_checkinout_date = False
      else:
          out_between_checkinout_date = False

      # check equal or gt date to check in
      equal_gt_date_to_checkin = check_equal_gt(req_date_to_check_in, date_to_check_in)

      # check equal or gt num of rooms
      equal_gt_numrooms = check_equal_gt(req_num_of_room, num_rooms)

      # check equal or gt num of nights
      equal_gt_numnights = check_equal_gt(req_num_of_night, num_of_nights)

      # check equal or gt rooms x nights
      equal_gt_roomsxnights = check_equal_gt(req_room_x_night, room_x_night)

      # not doing - check user login

      # iterate - check equal interface
      for i in range(0,len(req_intf_split)):
          if req_intf_split[i] == '*':
              equal_interface = True
              break
          elif interface == req_intf_split[i]:
              equal_interface = True
              break
          else:
              equal_interface = False

      # check equal or gt app version
      equal_gt_appversion = check_equal_gt(req_app_version, application_version)
      
      if  between_promo_date and \
          equal_hotel_id and \
          equal_hotel_brand and \
          equal_hotel_chain and \
          equal_hotel_area and \
          equal_hotel_city and \
          equal_hotel_region and \
          equal_hotel_country and \
          in_between_checkinout_date and \
          out_between_checkinout_date and \
          equal_gt_date_to_checkin and \
          equal_gt_numrooms and \
          equal_gt_numnights and \
          equal_gt_roomsxnights and \
          equal_locale and \
          equal_interface and \
          equal_gt_appversion:
              use_deals_flag = True
              if landing_page_flag == 'asdflkjhg':
                  landing_page_flag = landing_page
              else:
                  landing_page_flag = landing_page_flag + '~' + landing_page

  if use_deals_flag:
      if visit_deals_promo_page:
          split_landing_page_flag = landing_page_flag.split('~')
          for i in range(0,len(split_landing_page_flag)):
              # if split_landing_page_flag[i] in cleaned_landing_page:
              if '|' in split_landing_page_flag[i]:
                  split_flag = True
                  break
              else:
                  continue
                  
          if split_flag:
              writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
          else:
              writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': not visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and (not visit_deals_promo_page)), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
      else:
          writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
  else:
      writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})

print('DONE')

# COMMAND ----------

import csv
import datetime
import re
import time

from datetime import date
from datetime import datetime

def check_equal(req, value):
    if req == '*':
        return True
    elif value != 'undefined':
        if value == req:
            return True
        else:
            return False
    else:
        return False

def check_equal_gt(req, value):
    if req == '*':
        return True
    elif value != 'undefined':
        if value >= req:
            return True
        else:
            return False
    else:
        return False

csvlt = '\n'
csvdel = ','
csvquo = '"'

for row_joinres in hotel_issued_pandas.itertuples():
  visit_deals_promo_page = False
  split_flag = False
  use_deals_flag = False
  landing_page_flag = 'asdflkjhg'

  if row_joinres.booking_id is not None:
      booking_id = row_joinres.booking_id
  else:
      booking_id = 'undefined'

  if row_joinres.session_id is not None:
      session_id = row_joinres.session_id
  else:
      session_id = 'undefined'

  if row_joinres.issued_time is not None:
      issued_timestamp = row_joinres.issued_time
      issued_temp = time.mktime(time.strptime(issued_timestamp, '%Y-%m-%d %H:%M:%S')) + 25200
      issued_date_format = time.strftime('%Y-%m-%d', time.localtime(issued_temp))
  else:
      issued_timestamp = 'undefined'
      issued_date_format = 'undefined'

  if row_joinres.booking_time is not None:
      booking_timestamp = row_joinres.booking_time
      booking_temp = time.mktime(time.strptime(booking_timestamp, '%Y-%m-%d %H:%M:%S')) + 25200
      booking_date_format = time.strftime('%Y-%m-%d', time.localtime(booking_temp))
  else:
      booking_timestamp = 'undefined'
      booking_date_format = 'undefined'

  if row_joinres.check_in_date is not None:
      #check_in_timestamp = row_joinres.check_in_date
      #check_in_temp = time.mktime(time.strptime(check_in_timestamp, '%d-%m-%Y'))
      #check_in_date = time.strftime('%Y-%m-%d', time.localtime(check_in_temp))
      check_in_date = row_joinres.check_in_date
  else:
      check_in_date = 'undefined'

  if row_joinres.check_out_date is not None:
      #check_out_timestamp = row_joinres.check_out_date
      #check_out_temp = time.mktime(time.strptime(check_out_timestamp, '%d-%m-%Y'))
      #check_out_date = time.strftime('%Y-%m-%d', time.localtime(check_out_temp))
      check_out_date = row_joinres.check_out_date
  else:
      check_out_date = 'undefined'

  if issued_date_format != 'undefined' and check_in_date != 'undefined':
      issued_parse_temp = datetime.strptime(issued_date_format, '%Y-%m-%d')
      check_in_parse_temp = datetime.strptime(check_in_date, '%Y-%m-%d')
      date_to_check_in = int(abs((issued_parse_temp - check_in_parse_temp).days))
  else:
      date_to_check_in = 'undefined'

  if row_joinres.num_of_nights is not None:
      num_of_nights = int(row_joinres.num_of_nights)
  else:
      num_of_nights = 'undefined'

  if row_joinres.num_rooms is not None:
      num_rooms = int(row_joinres.num_rooms)
  else:
      num_rooms = 'undefined'

  if num_of_nights != 'undefined' and num_rooms != 'undefined':
      room_x_night = int(num_of_nights * num_rooms)
  else:
      room_x_night = 'undefined'

  if row_joinres.interface is not None:
      intf = row_joinres.interface
  else:
      intf = 'undefined'

  if row_joinres.app_version is not None:
      application_version = row_joinres.app_version
      application_version = application_version.replace(',','.')
      application_version = application_version.replace('.','')
      application_version = int(application_version)
      while(application_version < 100000):
          application_version *= 10
  else:
      application_version = 'undefined'

  if row_joinres.language_id is not None:
      lang = row_joinres.language_id
  else:
      lang = 'undefined'

  if row_joinres.country_id is not None:
      country = row_joinres.country_id
  else:
      country = 'undefined'

  if row_joinres.locale_id is not None:
      locale = row_joinres.locale_id
  elif lang != 'undefined' and country != 'undefined':
      locale = lang + "_" + country
  else:
      locale = 'undefined'

  if row_joinres.hotel_id is not None:
      hotel_id = row_joinres.hotel_id
  else:
      hotel_id = 'undefined'

  if row_joinres.brand_id is not None:
      hotel_brand = row_joinres.brand_id
  else:
      hotel_brand = 'undefined'

  if row_joinres.chain_id is not None:
      hotel_chain = row_joinres.chain_id
  else:
      hotel_chain = 'undefined'

  if row_joinres.area_name is not None:
      hotel_area = row_joinres.area_name
  else:
      hotel_area = 'undefined'

  if row_joinres.city_name is not None:
      hotel_city = row_joinres.city_name
  else:
      hotel_city = 'undefined'

  if row_joinres.region_name is not None:
      hotel_region = row_joinres.region_name
  else:
      hotel_region = 'undefined'

  if row_joinres.country_name is not None:
      hotel_country = row_joinres.country_name
  else:
      hotel_country = 'undefined'

  if row_joinres.lp_name is not None:
      landing_page = row_joinres.lp_name
  else:
      landing_page = 'undefined'

  if row_joinres.is_new_customer is not None:
      is_new_customer = row_joinres.is_new_customer.upper()
  else:
      is_new_customer = 'FALSE'

  if row_joinres.coupon_code is not None:
      coupon_code = row_joinres.coupon_code
  else:
      coupon_code = 'undefined'

  if row_joinres.installment_code is not None:
      installment_code = row_joinres.installment_code
  else:
      installment_code = 'undefined'

  for row_hotelreq in deals_req_pandas.itertuples():
      req_page_name = row_hotelreq.page_name
      req_routes_name = row_hotelreq.routes

      tempdate_start = row_hotelreq.start_promo_date
      if tempdate_start != '*':
          if '-' in tempdate_start:
              # tempdate_start = tempdate_start.split('-')
              start_date_temp = time.mktime(time.strptime(tempdate_start, '%Y-%m-%d'))
              req_start_promo_date = time.strftime('%Y-%m-%d', time.localtime(start_date_temp))
          if '/' in tempdate_start:
              # tempdate_start = tempdate_start.split('/')
              start_date_temp = time.mktime(time.strptime(tempdate_start, '%Y/%m/%d'))
              req_start_promo_date = time.strftime('%Y-%m-%d', time.localtime(start_date_temp))

          # date_start = int(tempdate_start[2])
          # month_start = int(tempdate_start[1])
          # year_start = int(tempdate_start[0])
          # req_start_promo_date = datetime.date(year_start, month_start, date_start)

      tempdate_end = row_hotelreq.end_promo_date
      if tempdate_end != '*':
          if '-' in tempdate_end:
              # tempdate_end = tempdate_end.split('-')
              end_date_temp = time.mktime(time.strptime(tempdate_end, '%Y-%m-%d'))
              req_end_promo_date = time.strftime('%Y-%m-%d', time.localtime(end_date_temp))
          if '/' in tempdate_end:
              # tempdate_end = tempdate_end.split('/')
              end_date_temp = time.mktime(time.strptime(tempdate_end, '%Y/%m/%d'))
              req_end_promo_date = time.strftime('%Y-%m-%d', time.localtime(end_date_temp))

          # date_end = int(tempdate_end[2])
          # month_end = int(tempdate_end[1])
          # year_end = int(tempdate_end[0])
          # req_end_promo_date = datetime.date(year_end, month_end, date_end)

      if row_hotelreq.hotel_id is not None:
          req_hotel_id = row_hotelreq.hotel_id
      else:
          req_hotel_id = '*'

      if row_hotelreq.hotel_brand is not None:
          req_hotel_brand = row_hotelreq.hotel_brand
      else:
          req_hotel_brand = '*'

      if row_hotelreq.hotel_chain is not None:
          req_hotel_chain = row_hotelreq.hotel_chain
      else:
          req_hotel_chain = '*'

      if row_hotelreq.hotel_area is not None:
          req_hotel_area = row_hotelreq.hotel_area
      else:
          req_hotel_area = '*'

      if row_hotelreq.hotel_city is not None:
          req_hotel_city = row_hotelreq.hotel_city
      else:
          req_hotel_city = '*'

      if row_hotelreq.hotel_region is not None:
          req_hotel_region = row_hotelreq.hotel_region
      else:
          req_hotel_region = '*'

      if row_hotelreq.hotel_country is not None:
          req_hotel_country = row_hotelreq.hotel_country
      else:
          req_hotel_country = '*'

      tempdate_checkin = row_hotelreq.check_in_date
      if tempdate_checkin != '*':
          if '-' in tempdate_checkin:
              # tempdate_checkin = tempdate_checkin.split('-')
              checkin_temp = time.mktime(time.strptime(tempdate_checkin, '%Y-%m-%d'))
              req_check_in_date = time.strftime('%Y-%m-%d', time.localtime(checkin_temp))
          if '/' in tempdate_checkin:
              # tempdate_checkin = tempdate_checkin.split('/')
              checkin_temp = time.mktime(time.strptime(tempdate_checkin, '%Y/%m/%d'))
              req_check_in_date = time.strftime('%Y-%m-%d', time.localtime(checkin_temp))
          
          # date_checkin = int(tempdate_checkin[2])
          # month_checkin = int(tempdate_checkin[1])
          # year_checkin = int(tempdate_checkin[0])
          # req_check_in_date = datetime.date(year_checkin, month_checkin, date_checkin)

      tempdate_checkout = row_hotelreq.check_out_date
      if tempdate_checkout != '*':
          if '-' in tempdate_checkout:
              # tempdate_checkout = tempdate_checkout.split('-')
              checkout_temp = time.mktime(time.strptime(tempdate_checkout, '%Y-%m-%d'))
              req_check_out_date = time.strftime('%Y-%m-%d', time.localtime(checkout_temp))
          if '/' in tempdate_checkout:
              # tempdate_checkout = tempdate_checkout.split('/')
              checkout_temp = time.mktime(time.strptime(tempdate_checkout, '%Y/%m/%d'))
              req_check_out_date = time.strftime('%Y-%m-%d', time.localtime(checkout_temp))

          # date_checkout = int(tempdate_checkout[2])
          # month_checkout = int(tempdate_checkout[1])
          # year_checkout = int(tempdate_checkout[0])
          # req_check_out_date = datetime.date(year_checkout, month_checkout, date_checkout)

      if row_hotelreq.date_to_check_in is not None:
          req_date_to_check_in = row_hotelreq.date_to_check_in
      else:
          req_date_to_check_in = '*'

      if row_hotelreq.num_of_room is not None:
          req_num_of_room = row_hotelreq.num_of_room
      else:
          req_num_of_room = '*'

      if row_hotelreq.num_of_night is not None:
          req_num_of_night = row_hotelreq.num_of_night
      else:
          req_num_of_night = '*'

      if row_hotelreq.room_x_night is not None:
          req_room_x_night = row_hotelreq.room_x_night
      else:
          req_room_x_night = '*'

      req_locale = row_hotelreq.locale
      req_locale_split = req_locale.split('|')

      req_intf = row_hotelreq.intf
      req_intf_split = req_intf.split('|')

      req_app_version = row_hotelreq.app_version
      if req_app_version != '*':
          req_app_version = req_app_version.replace(',','.')
          req_app_version = req_app_version.replace('.','')
          req_app_version = int(req_app_version)
          while(req_app_version < 100000):
              req_app_version *= 10

      # iterate - check equal locale
      for i in range(0,len(req_locale_split)):
          if req_locale_split[i] == '*':
              equal_locale = True
              break
          elif locale == req_locale_split[i]:
              equal_locale = True
              break
          else:
              equal_locale = False

      # check equal landing page
      if req_start_promo_date == '*' and req_end_promo_date == '*':
          visit_between_promo = True
      elif issued_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              visit_between_promo = req_start_promo_date <= issued_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              visit_between_promo = req_start_promo_date <= issued_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              visit_between_promo = issued_date_format <= req_end_promo_date
          else:
              visit_between_promo = False
      elif booking_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              visit_between_promo = req_start_promo_date <= booking_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              visit_between_promo = req_start_promo_date <= booking_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              visit_between_promo = booking_date_format <= req_end_promo_date
          else:
              visit_between_promo = False
      else:
          visit_between_promo = False
      
      cleaned_req_page_name = re.sub('[^0-9a-zA-Z]+', '', req_page_name.lower())
      cleaned_req_routes_name = re.sub('[^0-9a-zA-Z]+', '', req_routes_name.lower())
      cleaned_landing_page = re.sub('[^0-9a-zA-Z]+', '', landing_page.lower())
      # visit_deals_promo_page = req_landing_page_name.lower() in landing_page.lower()
      if ((cleaned_req_page_name in cleaned_landing_page) or (cleaned_req_routes_name in cleaned_landing_page)) and visit_between_promo and equal_locale:
          visit_deals_promo_page = True
          landing_page = req_routes_name + '|' + req_page_name

      # check booking between promo date
      if req_start_promo_date == '*' and req_end_promo_date == '*':
          between_promo_date = True
      elif booking_date_format != 'undefined':
          if req_start_promo_date != '*' and req_end_promo_date != '*':
              between_promo_date = req_start_promo_date <= booking_date_format <= req_end_promo_date
          elif req_start_promo_date != '*' and req_end_promo_date == '*':
              between_promo_date = req_start_promo_date <= booking_date_format
          elif req_start_promo_date == '*' and req_end_promo_date != '*':
              between_promo_date = booking_date_format <= req_end_promo_date
          else:
              between_promo_date = False
      else:
          between_promo_date = False

      # check equal hotel id
      equal_hotel_id = check_equal(req_hotel_id, hotel_id)

      # check equal hotel brand
      equal_hotel_brand = check_equal(req_hotel_brand, hotel_brand)

      # check equal hotel chain
      equal_hotel_chain = check_equal(req_hotel_chain, hotel_chain)

      # check equal hotel area
      equal_hotel_area = check_equal(req_hotel_area, hotel_area)

      # check equal hotel city
      equal_hotel_city = check_equal(req_hotel_city, hotel_city)

      # check equal hotel region
      equal_hotel_region = check_equal(req_hotel_region, hotel_region)

      # check equal hotel country
      equal_hotel_country = check_equal(req_hotel_country, hotel_country)

      # check check in date between check in and check out date
      if req_check_in_date == '*' and req_check_out_date == '*':
          in_between_checkinout_date = True
      elif check_in_date != 'undefined':
          if req_check_in_date != '*' and req_check_out_date != '*':
              in_between_checkinout_date = req_check_in_date <= check_in_date <= req_check_out_date
          elif req_check_in_date != '*' and req_check_out_date == '*':
              in_between_checkinout_date = req_check_in_date <= check_in_date
          elif req_check_in_date == '*' and req_end_promo_date != '*':
              in_between_checkinout_date = check_in_date <= req_check_out_date
          else:
              in_between_checkinout_date = False
      else:
          in_between_checkinout_date = False

      # check check out date between check in and check out date
      if req_check_in_date == '*' and req_check_out_date == '*':
          out_between_checkinout_date = True
      elif check_out_date != 'undefined':
          if req_check_in_date != '*' and req_check_out_date != '*':
              out_between_checkinout_date = req_check_in_date <= check_out_date <= req_check_out_date
          elif req_check_in_date != '*' and req_check_out_date == '*':
              out_between_checkinout_date = req_check_in_date <= check_out_date
          elif req_check_in_date == '*' and req_end_promo_date != '*':
              out_between_checkinout_date = check_out_date <= req_check_out_date
          else:
              out_between_checkinout_date = False
      else:
          out_between_checkinout_date = False

      # check equal or gt date to check in
      equal_gt_date_to_checkin = check_equal_gt(req_date_to_check_in, date_to_check_in)

      # check equal or gt num of rooms
      equal_gt_numrooms = check_equal_gt(req_num_of_room, num_rooms)

      # check equal or gt num of nights
      equal_gt_numnights = check_equal_gt(req_num_of_night, num_of_nights)

      # check equal or gt rooms x nights
      equal_gt_roomsxnights = check_equal_gt(req_room_x_night, room_x_night)

      # not doing - check user login

      # iterate - check equal interface
      for i in range(0,len(req_intf_split)):
          if req_intf_split[i] == '*':
              equal_interface = True
              break
          elif interface == req_intf_split[i]:
              equal_interface = True
              break
          else:
              equal_interface = False

      # check equal or gt app version
      equal_gt_appversion = check_equal_gt(req_app_version, application_version)
      
      if  between_promo_date and \
          equal_hotel_id and \
          equal_hotel_brand and \
          equal_hotel_chain and \
          equal_hotel_area and \
          equal_hotel_city and \
          equal_hotel_region and \
          equal_hotel_country and \
          in_between_checkinout_date and \
          out_between_checkinout_date and \
          equal_gt_date_to_checkin and \
          equal_gt_numrooms and \
          equal_gt_numnights and \
          equal_gt_roomsxnights and \
          equal_locale and \
          equal_interface and \
          equal_gt_appversion:
              use_deals_flag = True
              if landing_page_flag == 'asdflkjhg':
                  landing_page_flag = landing_page
              else:
                  landing_page_flag = landing_page_flag + '~' + landing_page

  if use_deals_flag:
      if visit_deals_promo_page:
          split_landing_page_flag = landing_page_flag.split('~')
          for i in range(0,len(split_landing_page_flag)):
              # if split_landing_page_flag[i] in cleaned_landing_page:
              if '|' in split_landing_page_flag[i]:
                  split_flag = True
                  break
              else:
                  continue
                  
  #         if split_flag:
  #             writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
  #         else:
  #             writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': not visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and (not visit_deals_promo_page)), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
  #     else:
  #         writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})
  # else:
  #     writer.writerow({'country': country, 'locale': locale, 'booking_id': booking_id, 'booking_date': issued_date_format, 'eligible_deals_promo': use_deals_flag, 'visit_deals_promo_page': visit_deals_promo_page, 'deals_promo_aware': (use_deals_flag and visit_deals_promo_page), 'deals_promo_type': landing_page, 'is_new_customer': is_new_customer, 'coupon_code': coupon_code, 'installment_code': installment_code})

print('DONE')

# COMMAND ----------


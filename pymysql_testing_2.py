import pymysql
import datetime

db = pymysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()

time = datetime.datetime.now()
current_version = 1

#tn_[verison int]_year_month_day_hour_minute

table_name = "tn_" + str(current_version) + "_" + str(time.year) + "_" + str(time.month) + "_" + str(time.day) + "_" + str(time.hour) + "_" + str(time.minute)

c.execute("CREAT TABLE " + table_name +"(number CHAR(255), count INT)")

db.commit()

db.close()
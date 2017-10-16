import pymysql

db = pymysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()
table_name = "tn_2_2017_10_14_20"
c.execute("SELECT * FROM " + table_name)
all_list = c.fetchall()
max_count = 0
max_count_num = ''
total_nums = 0

for i in all_list:
    total_nums += i[1]
    if i[1] >= max_count:
        max_count = i[1]
        max_count_num = i[0]

print(table_name)
print(max_count_num + ": " + str(max_count))
print("Total Numbers: " + str(total_nums))

db.close()
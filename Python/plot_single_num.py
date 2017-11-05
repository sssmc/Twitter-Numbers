import datetime

import plotly.plotly as py
import plotly.graph_objs as go
import pymysql

db = pymysql.connect("192.168.0.29", "sebastien", "climate", "test")
c = db.cursor()

lines = []
version = input("Version: ")
table_start_hour = int(input("Table Start Hour: "))
num_tables = int(input("Number of Tables: "))
year = input("Year: ")
month = input("Month: ")
day = input("Day: ")
num = int(input("Number To Plot: "))


def get_total_nums(table_name):
    c.execute("SELECT * FROM " + table_name)
    all_list = c.fetchall()
    total_nums = 0

    for i in all_list:
        total_nums += i[1]

    print("Total Numbers: " + str(total_nums))
    return total_nums


tables_l = []
counts_l = []
start_datetime = datetime.datetime(int(year),
                                   int(month),
                                   int(day),
                                   int(table_start_hour))
for x in range(table_start_hour, table_start_hour + num_tables):
    loop_datetime = start_datetime + datetime.timedelta(hours=x)
    table_name = "tn_" \
        + version + "_" \
        + str(loop_datetime.year) \
        + "_" + str(loop_datetime.month) \
        + "_" + str(loop_datetime.day) \
        + "_" + str(loop_datetime.hour)
    total_nums = get_total_nums(table_name)
    tables_l.append(x)

    c.execute("SELECT * FROM " + table_name + " WHERE number LIKE %s", (num))

    counts_l.append((c.fetchone()[1] / total_nums) * 100)


label_name = month + " " + day + " " + str(x) + "th hour for: " + str(num)
lines.append(go.Scatter(x=tables_l, y=counts_l, name=label_name))

layout = go.Layout(
    xaxis=dict(
        type='linear',
        autorange=True
    ),
    yaxis=dict(
        type='linear',
        autorange=True
    )
)

fig = go.Figure(data=lines, layout=layout)
file_name = table_name + "x " + str(num_tables) + " hours number: " + str(num)

print(py.plot(fig, filename=file_name))

db.close()

import sys
import plotly.plotly as py
import plotly.graph_objs as go
import pymysql

print(sys.executable)
print(sys.version)

db = pymysql.connect("192.168.0.29", "sebastien", "climate", "test")
c = db.cursor()

lines = []
version = input("Version: ")
table_start_hour = int(input("Table Start Hour: "))
num_tables = int(input("Number of Tables: "))
year = input("Year: ")
month = input("Month: ")
day = input("Day: ")
table_name = "tn_" + version + "_" + year + "_" + month + "_" + day + "_"
num_nums = int(input("Number of Numbers to Plot: "))
nums_l = []
for i in range(0, num_nums):
    nums_l.append(int(input("Number To Plot: ")))

print(nums_l)


def get_total_nums(table_name):
    c.execute("SELECT * FROM " + table_name)
    all_list = c.fetchall()
    total_nums = 0

    for i in all_list:
        total_nums += i[1]

    print("Total Numbers: " + str(total_nums))
    return total_nums


for i in nums_l:
    tables_l = []
    counts_l = []
    for x in range(table_start_hour, table_start_hour + num_tables):
        print(table_name + str(x))
        total_nums = get_total_nums(table_name + str(x))
        tables_l.append(x)

        c.execute("SELECT * FROM "
                  + table_name + str(x)
                  + " WHERE number LIKE %s", (i))

        counts_l.append((c.fetchone()[1] / total_nums) * 100)

    label_name = month + " " + day + " " + str(x) + "th hour for: " + str(i)
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
file_name = table_name
+ "x "
+ str(num_tables)
+ " hours number: " + str(nums_l)

print(py.plot(fig, filename=file_name))

db.close()

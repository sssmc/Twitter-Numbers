import plotly.plotly as py
import plotly.graph_objs as go
import pymysql


db = pymysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()

lines = []
version = input("Version: ")
table_start_hour = int(input("Table Start Hour: "))
num_tables = int(input("Number of Tables: "))
year = input("Year: ")
month = input("Month: ")
day = input("Day: ")
table_name = "tn_" + version + "_" + year +"_" + month + "_" + day + "_"

def get_total_nums(table_name):
    c.execute("SELECT * FROM " + table_name)
    all_list = c.fetchall()
    total_nums = 0

    for i in all_list:
        total_nums += i[1]

    print("Total Numbers: " + str(total_nums))
    return total_nums

for x in range(table_start_hour, table_start_hour + num_tables):
    print(table_name + str(x) + "_0")
    total_nums = get_total_nums(table_name + str(x))
    num_list = []
    count_list = []

    for i in range(0,101):
        try:
            c.execute("SELECT * FROM " + table_name + str(x) + " WHERE number LIKE %s", (i))
            num_list.append(c.fetchall()[0][0])
            c.execute("SELECT * FROM " + table_name + str(x) + " WHERE number LIKE %s", (i))
            count_list.append((c.fetchall()[0][1] / total_nums) * 100)
        except:
            #print("no data")
            num_list.append(i)
            count_list.append(0)
    print(num_list)
    print(count_list)

    label_name = "Oct. 13, " + str(x) +"th hour(%)"
    lines.append(go.Scatter(x=num_list, y=count_list, name=label_name))


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

print(py.plot(fig, filename='scatter log from full two hour data'))

db.close()
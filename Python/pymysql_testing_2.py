import pymysql
import datetime

db = pymysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()

version = 4
def write_metadata(version, table_name, is_complete, tweets=-1, tweets_w_nums=-1, total_nums=-1, dif_nums=-1, non_nums=-1, no_data_tweets=-1):
    #table name | complete | total tweets | total tweets with numbers | total numbers | number of differnt numbers | total of non numbers | total of no tweet data
    #tn_3_2017_10_19_24
    #000000000000000000000

    metadata_table_name = "tn_" + str(version) + "_metadata"

    print("Writing Metadate for Table: " + table_name)
    try:
        c.execute("SELECT * FROM " + metadata_table_name)
    except:
        print("Creating Metadata Table: " + metadata_table_name)
        c.execute("CREATE TABLE " + metadata_table_name+ "(version INT, table_name CHAR(21), is_complete TINYINT(1), tweets INT, tweets_w_nums INT, total_nums INT, dif_nums INT, non_nums INT, no_data_tweets INT)")
        db.commit()

    if c.execute("select * from tn_4_metadata where table_name like 'tn_3_2017_10_19_24'") == 0:
       print("Creating New Row")
       c.execute("INSERT INTO " + metadata_table_name + "(version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets))
    else:
       print("Updating Row")
       c.execute("UPDATE " + metadata_table_name + " SET version=%s, table_name=%s, is_complete=%s, tweets=%s, tweets_w_nums=%s, total_nums=%s, dif_nums=%s, non_nums=%s, no_data_tweets=%s", (version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets))


    #c.execute("INSERT INTO " + metadata_table_name + "(table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets))

    db.commit()

write_metadata(4, "tn_3_2017_10_19_24", 1, tweets=30000, tweets_w_nums=8768968)

db.close()
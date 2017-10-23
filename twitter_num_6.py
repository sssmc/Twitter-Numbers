import pymysql as pysql

import json
import re
import math
import time
import datetime
import threading

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

auth = OAuthHandler('rVBGZlv0fXG535e6XtNFTHKFB', 'a8uK4zSu5uku5mpOXggQxa9k29QDF4aEjbAWDOuBDZX1h59WhT')
auth.set_access_token('3260207947-ieMSsYpzSwuCkM5ceM4c8AudzQv0QHpElLULAFa', 'PaWOAasOFoPFDdYMARVr31mK1iy35sOqhZzpr1fDmNkZX')

queue = []

#tweets tweets_w_nums total_nums non_nums no_data_tweets
tweet_data = [0,0,0,0,0]

current_version = 6#must be int

print("Twitter Numbers Version: " + str(current_version))

class myThread (threading.Thread):
   def __init__(self, threadID, name, counter):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
      self.counter = counter
   def run(self):
    if self.threadID == 1:
        print("Main Loop Thread Starting")
        main_loop()
        print("Main Loop Thread Ending")
    elif self.threadID == 2:
        print("Streaming Thread Starting")
        while True:
            try:
                print("Start Streaming")
                stream = twitterStream = Stream(auth, listener())
                twitterStream.sample()
            except Exception as e:
                print("Error. Restarting Stream.... Error: ")
                print(e.__doc__)
                print("Error Message")
                time.sleep(5)
        print("Streaming Thread Ending")

class listener(StreamListener):

    def __init__(self, path=None):
        self.path = path
        self.rec_tries_420 = 0
        self.rec_tries_other = 0

    def on_data(self, data):

        update_tweet_data(0)

        all_data = json.loads(data)

        pro_l = process_tweet(all_data)

        if len(pro_l) != 0:
            update_tweet_data(1)

        for s in pro_l:
            update_tweet_data(2)
            add_to_queue(s)


        return(True)

    def on_error(self, status):
        print("error code: " + str(status))
        if status == 420:
            sleep_time = 60 * math.pow(2, self.rec_tries_420)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print("Recnnecting in: ")
            print(str(sleep_time/60) + " minutes")
            print("'''")
            time.sleep(sleep_time)
            self.rec_tries_420 += 1
        else:
            sleepy = 5
            print(time.strftime("%Y%m%d_%H%M%S"))
            print ("A reconnection attempt will occur in " + str(sleepy) + " seconds.")
            time.sleep(sleepy)

        return True

def add_to_queue(num):
    global queue
    queue.append(num)

def update_tweet_data(index):
    #tweets tweets_w_nums total_nums non_nums no_data_tweets
    global tweet_data
    tweet_data[index] += 1

def process_tweet(tweet):
    try:
        pro_tweet_l = re.findall(r'\b\d+\b',tweet["text"])
    except:
        update_tweet_data(4)
        pro_tweet_l = []

    return pro_tweet_l

def add_to_db(num, current_table_name):
    try:

        if c.execute("SELECT * FROM " + current_table_name + " WHERE number LIKE %s", (num)) == 0:
            c.execute("INSERT INTO " + current_table_name + " (number,count) VALUES (%s,1)", (num))
        else:
            c.execute("SELECT count From " + current_table_name + " WHERE number LIKE %s", (num))
            c.execute("SELECT count From " + current_table_name + " WHERE number LIKE %s", (num))
            new_count = int(c.fetchone()[0])
            new_count += 1
            c.execute("UPDATE " + current_table_name + " SET count='%s' WHERE number LIKE %s", (new_count,num))

        db.commit()
    except:
        print("Error Skipping")
        update_tweet_data(3)

def create_table(time, is_complete, prv_table_name = "", old_table_name=""):
    table_name = "tn_" + str(current_version) + "_" + str(time.year) + "_" + str(time.month) + "_" + str(time.day) + "_" + str(time.hour)
    #tweets tweets_w_nums total_nums non_nums no_data_tweets
    global tweet_data

    print("Creating Table: " + table_name)

    c.execute("CREATE TABLE " + table_name +"(number CHAR(255), count INT)")

    if prv_table_name != "" and old_table_name != "":
        print("Writing Tweet Data: " + str(tweet_data) + " for table :" + old_table_name)
        write_metadata(current_version, old_table_name, is_complete, tweets=tweet_data[0], tweets_w_nums=tweet_data[1], total_nums=tweet_data[2], non_nums=tweet_data[3], no_data_tweets=tweet_data[4])
        tweet_data = [0,0,0,0,0]

    if time.minute == 0:
        write_metadata(current_version,table_name,1)
    else:
        write_metadata(current_version,table_name,0)

    db.commit()
    return table_name

def write_metadata(version, table_name, is_complete, tweets=-1, tweets_w_nums=-1, total_nums=-1 , non_nums=-1, no_data_tweets=-1):
    #table name | complete | total tweets | total tweets with numbers | total numbers | number of differnt numbers | total of non numbers | total of no tweet data
    #tn_3_2017_10_19_24
    #000000000000000000000

    metadata_table_name = "tn_" + str(version) + "_metadata"

    dif_nums = c.execute("SELECT * FROM " + table_name)

    print("Writing Metadate for Table: " + table_name)
    try:
        c.execute("SELECT * FROM " + metadata_table_name)
    except:
        print("Creating Metadata Table: " + metadata_table_name)
        c.execute("CREATE TABLE " + metadata_table_name+ "(version INT, table_name CHAR(21), is_complete TINYINT(1), tweets INT, tweets_w_nums INT, total_nums INT, dif_nums INT, non_nums INT, no_data_tweets INT)")
        db.commit()

    if c.execute("select * from " + metadata_table_name + " where table_name like '" + table_name +"'") == 0:
       print("Creating New Row")
       c.execute("INSERT INTO " + metadata_table_name + "(version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets))
    else:
       print("Updating Row")
       c.execute("UPDATE " + metadata_table_name + " SET version=%s, table_name=%s, is_complete=%s, tweets=%s, tweets_w_nums=%s, total_nums=%s, dif_nums=%s, non_nums=%s, no_data_tweets=%s WHERE table_name=%s", (version, table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets, table_name))


    #c.execute("INSERT INTO " + metadata_table_name + "(table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (table_name, is_complete, tweets, tweets_w_nums, total_nums, dif_nums, non_nums, no_data_tweets))

    db.commit()


def main_loop():
    current_time = datetime.datetime.utcnow()
    table_start_hour = current_time.hour
    try:
        if current_time.minute == 0:
            current_table_name = create_table(current_time, 1)
            is_complete = 1
        else:
            current_table_name = create_table(current_time, 0)
            is_complete = 0
    except pysql.DatabaseError:
        print("database already created")
        current_table_name = "tn_" + str(current_version) + "_" + str(current_time.year) + "_" + str(current_time.month) + "_" + str(current_time.day) + "_" + str(current_time.hour)
        write_metadata(current_version, current_table_name,0)
        is_complete = 0
        pass

    try:
        while True:

            current_time = datetime.datetime.utcnow()
            if current_time.hour != table_start_hour:
                print("creating new table")
                try:
                    old_table_name = current_table_name
                    current_table_name = create_table(current_time,is_complete, prv_table_name=old_table_name, old_table_name=old_table_name)
                    is_complete = 1
                except pysql.DatabaseError as error:
                    print("database already created(in loop)")
                    print("error: " + str(error))
                    current_table_name = "tn_" + str(current_version) + "_" + str(current_time.year) + "_" + str(current_time.month) + "_" + str(current_time.day) + "_" + str(current_time.hour)
                table_start_hour = current_time.hour

            if(len(queue) > 0):
                num = str(queue[0])

                if num.isdigit():
                    print("Number: "+num)
                    add_to_db(num, current_table_name)
                else:
                    print("Not a number: " + num)
                del queue[0]
                #print(len(queue))
    except KeyboardInterrupt:
        print("closing database")
        db.close()

db = pysql.connect("192.168.0.29", "sebastien", "climate", "test")
c = db.cursor()

# Create new threads
thread1 = myThread(1, "Thread_main_loop", 1)
thread2 = myThread(2, "Thread_streaming", 2)

# Start new Threads
thread1.start()
thread2.start()
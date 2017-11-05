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

current_version = 2#must be int

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

        all_data = json.loads(data)

        pro_l = process_tweet(all_data)

        for s in pro_l:
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
    global queue
    queue.append(num)

def process_tweet(tweet):
    try:
        pro_tweet_l = re.findall(r'\b\d+\b',tweet["text"])
    except:
        pro_tweet_l = []

    return pro_tweet_l

def add_to_db(num, current_table_name):
    try:

        if c.execute("SELECT * FROM " + current_table_name + " WHERE number LIKE %s", (num)) == 0:
            #print("New Number")
            c.execute("INSERT INTO " + current_table_name + " (number,count) VALUES (%s,1)", (num))
        else:
            #print("Exsiting Number")
            c.execute("SELECT count From " + current_table_name + " WHERE number LIKE %s", (num))
            #print("Old Count: " + str(c.fetchone()[0]))
            c.execute("SELECT count From " + current_table_name + " WHERE number LIKE %s", (num))
            new_count = int(c.fetchone()[0])
            new_count += 1
            #print("New Count: " + str(new_count))
            c.execute("UPDATE " + current_table_name + " SET count='%s' WHERE number LIKE %s", (new_count,num))

        db.commit()
    except:
        print("Error Skipping")

def create_table(time):
    #create table
    #todo check if table exsists
    #tn_[verison int]_year_month_day_hour_minute

    table_name = "tn_" + str(current_version) + "_" + str(time.year) + "_" + str(time.month) + "_" + str(time.day) + "_" + str(time.hour)# + "_" + str(time.minute)

    print("Creating Table: " + table_name)

    c.execute("CREATE TABLE " + table_name +"(number CHAR(255), count INT)")

    db.commit()
    return table_name

def main_loop():
    ##
    current_time = datetime.datetime.now()
    table_start_hour = current_time.hour
    try:
        current_table_name = create_table(current_time)
    except pysql.DatabaseError:
        print("database already created")
        current_table_name = "tn_" + str(current_version) + "_" + str(current_time.year) + "_" + str(current_time.month) + "_" + str(current_time.day) + "_" + str(current_time.hour)
        pass

    try:
        while True:

            current_time = datetime.datetime.now()
            if current_time.hour != table_start_hour:
                print("creating new table")
                try:
                    current_table_name = create_table(current_time)
                except pysql.DatabaseError:
                    print("database already created(in loop)")
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

db = pysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()

# Create new threads
thread1 = myThread(1, "Thread_main_loop", 1)
thread2 = myThread(2, "Thread_streaming", 2)

# Start new Threads
thread1.start()
thread2.start()

# main_loop()
#
# stream = twitterStream = Stream(auth, listener())
# twitterStream.sample(async=True)



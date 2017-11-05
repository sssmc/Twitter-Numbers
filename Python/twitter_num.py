import json
import re
import math
import time
import datetime
import threading
import logging

import pymysql
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener

current_version = 10  # must be int
auth = OAuthHandler('rVBGZlv0fXG535e6XtNFTHKFB',
                    'a8uK4zSu5uku5mpOXggQxa9k29QDF4aEjbAWDOuBDZX1h59WhT')
auth.set_access_token('3260207947-ieMSsYpzSwuCkM5ceM4c8AudzQv0QHpElLULAFa',
                      'PaWOAasOFoPFDdYMARVr31mK1iy35sOqhZzpr1fDmNkZX')

logging.basicConfig(filename='tn_' + str(current_version) + '.log',
                    level=logging.INFO,
                    format="%(asctime)s:%(levelname)s:%(message)s")
# tweets tweets_w_nums total_nums non_nums no_data_tweets
stream_speed_divider = 2
queue = []
tweet_data = [0, 0, 0, 0, 0]
database_latency_min = -1
database_latency_max = 0
database_latency_avg_sum = 0
database_latency_num = 1
processing_latency_min = -1
processing_latency_max = 0
processing_latency_avg_sum = 0
processing_latency_num = 1

logging.info("Starting Twitter Numbers Version: " + str(current_version))
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
                    logging.info("Start Streaming")
                    twitterStream = Stream(auth, listener())
                    twitterStream.sample()

                except Exception as e:

                    print("Error. Restarting Stream.... Error: ")
                    print(e.__doc__)
                    print("Error Message")
                    set_is_complete(0)
                    logging.error("Streaming Error: "
                                  + str(e)
                                  + ": " + str(e.__doc__))
                    time.sleep(5)
            print("Streaming Thread Ending")


class listener(StreamListener):

    def __init__(self, path=None):
        self.path = path
        self.rec_tries_420 = 0
        self.rec_tries_other = 0
        self.data_count = 0

    def on_data(self, data):

        if self.data_count == 0:
            d1 = datetime.datetime.now()
            update_tweet_data(0)

            all_data = json.loads(data)

            pro_l = process_tweet(all_data)

            if len(pro_l) != 0:
                update_tweet_data(1)
            for s in pro_l:
                update_tweet_data(2)
                add_to_queue(s)

            self.data_count = get_stream_speed_divider()
            d2 = datetime.datetime.now()
            processing_latency((d2 - d1).microseconds)
            # print((d2 - d1))
        else:
            self.data_count -= 1
        return(True)

    def on_error(self, status):
        print("error code: " + str(status))
        logging.error("Stream Connection Error: " + str(status))
        if status == 420:
            sleep_time = 60 * math.pow(2, self.rec_tries_420)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print("Recnnecting in: ")
            print(str(sleep_time / 60) + " minutes")
            logging.info("Reconnecting in: "
                         + str(sleep_time / 60)
                         + "minutes")
            print("'''")
            time.sleep(sleep_time)
            self.rec_tries_420 += 1
        else:
            sleepy = 5
            print(time.strftime("%Y%m%d_%H%M%S"))
            print("A reconnection attempt will occur in "
                  + str(sleepy)
                  + " seconds.")
            logging.info("Reconnecting in: " + str(sleepy))
            time.sleep(sleepy)

        return True


def get_stream_speed_divider():
    global stream_speed_divider
    return stream_speed_divider


def add_to_queue(num):
    global queue
    queue.append(num)


def update_tweet_data(index):
    # tweets tweets_w_nums total_nums non_nums no_data_tweets
    global tweet_data
    tweet_data[index] += 1


def set_is_complete(set_is_complete):
    global is_complete
    is_complete = set_is_complete


def database_latency(time):
    global database_latency_min
    global database_latency_max
    global database_latency_avg_sum
    global database_latency_num
    database_latency_num += 1
    database_latency_avg_sum += time
    if database_latency_min == -1:
        database_latency_min = time
    elif time < database_latency_min:
        database_latency_min = time
    if time > database_latency_max:
        database_latency_max = time


def processing_latency(time):
    global processing_latency_min
    global processing_latency_max
    global processing_latency_avg_sum
    global processing_latency_num
    processing_latency_num += 1
    processing_latency_avg_sum += time
    if processing_latency_min == -1:
        processing_latency_min = time
    elif time < processing_latency_min:
        processing_latency_min = time
    if time > processing_latency_max:
        processing_latency_max = time


def process_tweet(tweet):
    try:
        # pro_tweet_l = re.findall(r'\b\d+\b', tweet["text"])
        pro_tweet_l = re.findall('[-+]?[.]?[\d]+(?:,\d\d\d)*'
                                 '[\.]?\d*(?:[eE][-+]?\d+)?',
                                 tweet["text"])
        for n in range(0, len(pro_tweet_l)):
            pro_tweet_l[n] = str(pro_tweet_l[n])
            pro_tweet_l[n] = pro_tweet_l[n].replace(",", "")
            if pro_tweet_l[n][len(pro_tweet_l[n]) - 1] == '.':
                pro_tweet_l[n] = pro_tweet_l[n][:-1]
            if '.' in pro_tweet_l[n]:
                pro_tweet_l[n] = float(pro_tweet_l[n])
                pro_tweet_l[n] = str(pro_tweet_l[n])
    except:
        update_tweet_data(4)
        pro_tweet_l = []
    return pro_tweet_l


def add_to_db(num, current_table_name):
    try:
        if c.execute("SELECT * FROM "
                     + current_table_name
                     + " WHERE number LIKE %s", (num)) == 0:
            c.execute("INSERT INTO "
                      + current_table_name
                      + " (number,count) VALUES (%s,1)", (num))
        else:
            c.execute("SELECT count From "
                      + current_table_name
                      + " WHERE number LIKE %s", (num))
            c.execute("SELECT count From "
                      + current_table_name
                      + " WHERE number LIKE %s", (num))
            new_count = int(c.fetchone()[0])
            new_count += 1
            c.execute("UPDATE "
                      + current_table_name
                      + " SET count='%s' WHERE number LIKE %s",
                      (new_count, num))
        db.commit()
    except UnicodeEncodeError as e:
        print("Error Skipping: " + str(e))
        update_tweet_data(3)
        pass


def create_table(time, is_complete, prv_table_name="", old_table_name=""):
    table_name = "tn_" + str(current_version) + "_" + str(time.year) + "_" + \
        str(time.month) + "_" + str(time.day) + "_" + str(time.hour)
    # tweets tweets_w_nums total_nums non_nums no_data_tweets
    global tweet_data
    global processing_latency_min
    global processing_latency_max
    global processing_latency_avg_sum
    global processing_latency_num
    global database_latency_min
    global database_latency_max
    global database_latency_avg_sum
    global database_latency_num
    print("Creating Table: " + table_name)
    c.execute("CREATE TABLE " + table_name + "(number CHAR(255), count INT)")
    logging.info("Creating Table: " + table_name)
    if prv_table_name != "" and old_table_name != "":
        print("Writing Tweet Data: "
              + str(tweet_data)
              + " for table :"
              + old_table_name)
        write_metadata(current_version,
                       old_table_name, is_complete,
                       tweets=tweet_data[0],
                       tweets_w_nums=tweet_data[1],
                       total_nums=tweet_data[2],
                       non_nums=tweet_data[3],
                       no_data_tweets=tweet_data[4])
        tweet_data = [0, 0, 0, 0, 0]
        database_latency_min = -1
        database_latency_max = 0
        database_latency_avg_sum = 0
        database_latency_num = 1
        processing_latency_min = -1
        processing_latency_max = 0
        processing_latency_avg_sum = 0
        processing_latency_num = 1
    if time.minute == 0:
        write_metadata(current_version, table_name, 1)
    else:
        write_metadata(current_version, table_name, 0)

    db.commit()
    return table_name


def write_metadata(version,
                   table_name,
                   is_complete,
                   tweets=-1,
                   tweets_w_nums=-1,
                   total_nums=-1,
                   non_nums=-1,
                   no_data_tweets=-1):
    # table name | complete | total tweets | total tweets with numbers
    # | total numbers | number of differnt numbers | total of non numbers
    # | total of no tweet data | processing_latency_min |
    # processing_latency_max
    # | processing_latency_avg | database_latency_min database_latency_max
    # | database_latency_avg
    # tn_3_2017_10_19_24
    # 000000000000000000000
    metadata_table_name = "tn_" + str(version) + "_metadata"
    dif_nums = c.execute("SELECT * FROM " + table_name)
    database_latency_avg = database_latency_avg_sum / database_latency_num
    processing_latency_avg = \
        processing_latency_avg_sum / processing_latency_num
    print("Writing Metadate for Table: " + table_name)
    logging.info("Writing Metadate for Table: " + table_name)
    try:
        c.execute("SELECT * FROM " + metadata_table_name)
    except pymysql.DatabaseError:
        print("Creating Metadata Table: " + metadata_table_name)
        logging.info("Creating Metadata Table: " + metadata_table_name)
        c.execute("CREATE TABLE "
                  + metadata_table_name
                  + "(version INT, table_name CHAR(21), "
                  "is_complete TINYINT(1), tweets INT, "
                  "tweets_w_nums INT, "
                  "total_nums INT, "
                  "dif_nums INT, "
                  "non_nums INT, "
                  "no_data_tweets INT, "
                  "processing_latency_min INT, "
                  "processing_latency_max INT, "
                  "processing_latency_avg INT, "
                  "database_latency_min INT, "
                  "database_latency_max INT, "
                  "database_latency_avg INT, "
                  "stream_speed_divider INT)")
        db.commit()

    if c.execute("select * from "
                 + metadata_table_name
                 + " where table_name like '"
                 + table_name + "'") == 0:
        print("Creating New Row")
        c.execute("INSERT INTO "
                  + metadata_table_name
                  + "(version, table_name, is_complete, tweets, tweets_w_nums,"
                  " total_nums, dif_nums, non_nums, no_data_tweets, "
                  "processing_latency_min, processing_latency_max, "
                  "processing_latency_avg, database_latency_min, "
                  "database_latency_max, database_latency_avg, "
                  "stream_speed_divider)"
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, "
                  "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                  (version,
                    table_name,
                    is_complete,
                    tweets,
                    tweets_w_nums,
                    total_nums,
                    dif_nums,
                    non_nums,
                    no_data_tweets,
                    processing_latency_min,
                    processing_latency_max,
                    processing_latency_avg,
                    database_latency_min,
                    database_latency_max,
                    database_latency_avg,
                    stream_speed_divider))
    else:
        print("Updating Row")
        c.execute("UPDATE "
                  + metadata_table_name
                  + " SET version=%s, "
                  "table_name=%s, "
                  "is_complete=%s, "
                  "tweets=%s, "
                  "tweets_w_nums=%s, "
                  "total_nums=%s, "
                  "dif_nums=%s, "
                  "non_nums=%s, "
                  "no_data_tweets=%s, "
                  "processing_latency_min=%s, "
                  "processing_latency_max=%s, "
                  "processing_latency_avg=%s, "
                  "database_latency_min=%s, "
                  "database_latency_max=%s, "
                  "database_latency_avg=%s, "
                  "stream_speed_divider=%s "
                  "WHERE table_name=%s",
                  (version,
                    table_name,
                    is_complete,
                    tweets,
                    tweets_w_nums,
                    total_nums,
                    dif_nums,
                    non_nums,
                    no_data_tweets,
                    processing_latency_min,
                    processing_latency_max,
                    processing_latency_avg,
                    database_latency_min,
                    database_latency_max,
                    database_latency_avg,
                    stream_speed_divider,
                    table_name))
    # c.execute("INSERT INTO " + metadata_table_name


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
    except pymysql.DatabaseError:
        print("table already created")
        logging.info("Table Already Created")
        current_table_name = "tn_" \
            + str(current_version) \
            + "_" + str(current_time.year) \
            + "_" + str(current_time.month) \
            + "_" + str(current_time.day) \
            + "_" + str(current_time.hour)
        write_metadata(current_version, current_table_name, 0)
        is_complete = 0
        pass
    try:
        queue_len_warning = False
        while True:
            current_time = datetime.datetime.utcnow()
            if current_time.hour != table_start_hour:
                print("creating new table")
                try:
                    old_table_name = current_table_name
                    current_table_name = \
                        create_table(current_time,
                                     is_complete,
                                     prv_table_name=old_table_name,
                                     old_table_name=old_table_name)
                    is_complete = 1
                    if queue_len_warning is True:
                        print("Queue Length Above 100: " + str(len(queue)))
                        logging.warning("Queue Length Above 100: "
                                        + str(len(queue)))
                except pymysql.DatabaseError as error:
                    print("database already created(in loop)")
                    print("error: " + str(error))
                    logging.info("Table Already Created(loop)")
                    current_table_name = "tn_" \
                        + str(current_version) \
                        + "_" + str(current_time.year) \
                        + "_" + str(current_time.month) \
                        + "_" + str(current_time.day) \
                        + "_" \
                        + str(current_time.hour)
                table_start_hour = current_time.hour
            if(len(queue) > 0):
                print("Number: " + queue[0])
                d3 = datetime.datetime.now()
                num = str(queue[0])
                add_to_db(num, current_table_name)
                del queue[0]
                d4 = datetime.datetime.now()
                database_latency((d4 - d3).microseconds)
                if len(queue) > 100:
                    if queue_len_warning is False:
                        print("Queue Length Above 100: " + str(len(queue)))
                        logging.warning("Queue Length Above 100: "
                                        + str(len(queue)))
                        queue_len_warning = True
                if len(queue) < 90 and queue_len_warning is True:
                    print("Queue Length Under 90")
                    logging.warning("Queue Length Under 90")
                    queue_len_warning = False
    except KeyboardInterrupt:
        print("closing database")
        db.close()


db = pymysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()
# Create new threads
thread1 = myThread(1, "Thread_main_loop", 1)
thread2 = myThread(2, "Thread_streaming", 2)
# Start new Threads
thread1.start()
thread2.start()

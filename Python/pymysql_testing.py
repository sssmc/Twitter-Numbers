import pymysql as pysql

import json
import re
import math
import time

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

auth = OAuthHandler('rVBGZlv0fXG535e6XtNFTHKFB', 'a8uK4zSu5uku5mpOXggQxa9k29QDF4aEjbAWDOuBDZX1h59WhT')
auth.set_access_token('3260207947-ieMSsYpzSwuCkM5ceM4c8AudzQv0QHpElLULAFa', 'PaWOAasOFoPFDdYMARVr31mK1iy35sOqhZzpr1fDmNkZX')

queue = []

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

def add_to_db(num):

    try:

        if c.execute("SELECT * FROM no_time WHERE number LIKE %s", (num)) == 0:
            #print("New Number")
            c.execute("INSERT INTO no_time (number,count) VALUES (%s,1)", (num))
        else:
            #print("Exsiting Number")
            c.execute("SELECT count From no_time WHERE number LIKE %s", (num))
            #print("Old Count: " + str(c.fetchone()[0]))
            c.execute("SELECT count From no_time WHERE number LIKE %s", (num))
            new_count = int(c.fetchone()[0])
            new_count += 1
            #print("New Count: " + str(new_count))
            c.execute("UPDATE no_time SET count='%s' WHERE number LIKE %s", (new_count,num))

        db.commit()
    except:
        print("Database encoding error, skipping")


stream = twitterStream = Stream(auth, listener())
twitterStream.sample(async=True)

db = pysql.connect("localhost", "root", "Climate123", "test")
c = db.cursor()

try:
    while True:
        if(len(queue) > 0):
            num = str(queue[0])

            if num.isdigit():
                print("Number: "+num)
                add_to_db(num)
            else:
                print("Not a number: " + num)
            del queue[0]
            #print(len(queue))
except KeyboardInterrupt:
    print("closing database")
    db.close()
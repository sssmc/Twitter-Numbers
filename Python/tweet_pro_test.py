import re
text = "this is a tweet in 2017. looking forward to 2018 and 5798437598 1"

def process_tweet(tweet):

   pro_tweet_l = tweet.split()

   pro_tweet_l = re.findall(r'\b\d+\b',tweet)

   return pro_tweet_l

for s in process_tweet(text):
   print(s)

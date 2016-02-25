import json
import glob
import sys
import datetime
import time
from collections import defaultdict
from StringIO import StringIO
from zipfile import ZipFile
from urllib import urlopen


def filterTweet(tweet):
    for text in ["http", "www"]:
        if text in tweet:
            return False
    
    for text in ["i feel", "i am feeling", "i'm feeling", "i dont feel", "i'm", "i am", "makes me"]:
        if text in tweet:
            return True
    if "im" in tweet.split():
        return True
    return False

def seperateTweetsByTime(date, tweets):
    tempDict = defaultdict(list)
    keys = {14 : "8-9", 15 : "9-10", 16 : "10-11", 17 : "11-12", 18 : "12-13", 19 : "13-14", 20 : "14-15", 21 : "15-16"}
    
    temp = []
    for tweet in tweets:
        created_at = tweet['created_at']
        myTime = time.strptime(created_at,'%a %b %d %H:%M:%S +0000 %Y')
        if int(myTime.tm_hour) in keys.keys():
            tempDict[keys[int(myTime.tm_hour)]].append(tweet)
    return tempDict

# Reads the tweets from the given JSON file.
def readAllTweetsOfSpecifiedDate(date):
    allTweets = []
    allFiles = [f for f in glob.glob("RawData/"+ date + "/*")]
    allFiles.remove("RawData/"+ date + "/" + date + ".log")
    for fileName in allFiles:
        with open(fileName) as f:
            for line in f:
                tweet = json.loads(line)
                if filterTweet(tweet['text'].lower()):
                    allTweets.append(tweet)
    return allTweets


def afinn_sentiment(terms, afinn):
    pos = 0
    neg = 0
    for t in terms:
        if t in afinn:
            if afinn[t] > 0:
                pos += afinn[t]
            else:
                neg += -1 * afinn[t]
    return pos, neg


def main(date):
    
    url = urlopen('http://www2.compute.dtu.dk/~faan/data/AFINN.zip')
    zipfile = ZipFile(StringIO(url.read()))
    afinn_file = zipfile.open('AFINN/AFINN-111.txt')

    afinn = dict()

    for line in afinn_file:
        parts = line.strip().split()
        if len(parts) == 2:
            afinn[parts[0]] = int(parts[1])

    print 'read', len(afinn)
    
    
    
    
    tweets = readAllTweetsOfSpecifiedDate(date)
    result = seperateTweetsByTime(date, tweets)
    print "Total Tweets:", len(tweets)
    for time, timeTweets in result.items():
        file = open("train/" + date + "_" + time + ".txt", "a+")
        for tweet in timeTweets:
            pos, neg = afinn_sentiment(tweet['text'].split(), afinn)
            if (pos != 0) or (neg != 0):
                file.write(tweet['text'].encode('utf8','replace') + '\n')
        file.close()
    print "Done filtering..."

if __name__ == "__main__":
    main(sys.argv[1])

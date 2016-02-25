import ConfigParser
import sys
import time
import json
import schedule
import datetime
import os
import logging

from TwitterAPI import TwitterAPI

logger = logging.getLogger('Collect Tweets')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('mylog.log')
fh.setLevel(logging.DEBUG)

#ch = logging.StreamHandler()
#ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#ch.setFormatter(formatter)
fh.setFormatter(formatter)

#logger.addHandler(ch)
logger.addHandler(fh)

def get_twitter(config_file):
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    twitter = TwitterAPI(
                         config.get('twitter', 'consumer_key'),
                         config.get('twitter', 'consumer_secret'),
                         config.get('twitter', 'access_token'),
                         config.get('twitter', 'access_token_secret'))
    log = "[TWITTER]     : " + "Twitter connection established."
    print log
    logger.debug(log)
    return twitter


def getTweets(twitter, dir, date, limit = 100, abort_after=60):
    filename = dir + os.sep + date + "_1.json"
    outfile = open(filename , 'a+')
    log = "[FILE]        : " + "File " + filename + " created."
    print log
    logger.debug(log)
    fileCount = 1
    timeout = time.time() + abort_after
    tweetCount = 0
    while True:
        try:
            for response in twitter.request('statuses/filter', {'locations':'-124.637,24.548,-66.993,48.9974'}):
                if 'text' in response.keys():
                    json.dump(response, outfile)
                    outfile.write('\n')
                tweetCount += 1
                if tweetCount % 100 == 0:
                    log = "[TWITTER]     : " + "Found " + str(tweetCount) + " tweets."
                    print log
                    logger.info(log)
                if tweetCount % 100 == 0:
                    fileCount += 1
                    outfile.close()
                    filename = dir + os.sep + date + "_" + str(fileCount) + ".json"
                    outfile = open(filename, 'a+')
                    log = "[FILE]        : " + "File " + filename + " created."
                    print log
                    logger.debug(log)
                if time.time() > timeout:
                    outfile.close()
                    log = "[TIME]        : " + "Timeout."
                    print log
                    logger.info(log)
                    return
#                 if tweetCount >= limit:
#                     outfile.close()
#                     return
        except:
            log = "[ERROR]       : " + str(sys.exc_info()[0]) + "."
            print log
            logger.error(log)
    outfile.close()
    return

def job():
    todaysDate = datetime.datetime.now()
    date = todaysDate.strftime("%d") + todaysDate.strftime("%b").upper()
    dir = "data" + os.sep + date
    log = "[JOB]         : " + "Starting Tweets Collection Job on " + str(todaysDate.strftime("%d")) + " " + todaysDate.strftime("%B") + "."
    print log
    logger.debug(log)
    twitter = get_twitter('twitter.cfg')
    makeDir(dir)
    getTweets(twitter, dir, date, limit = 1000, abort_after=20)
    log = "[JOB]         : " + "Stopping Tweets Collection Job on " + str(todaysDate.strftime("%d")) + " " + todaysDate.strftime("%B") + "."
    print log
    logger.debug(log)
    return


def makeDir(path):
    try:
        os.makedirs(path)
    except OSError:
        log = "[ERROR]       : " + str(sys.exc_info()[0]) + "."
        print log
        logger.error(log)
#    os.chdir(path)


def main():
    log = "[PROGRAM]     : " + "Starting Program."
    print log
    logger.info(log)
    
    scheduledTime = "01:40"
    log = "[SCHEDULE]    : " + "Scheduled job at " + scheduledTime + "."
    print log
    logger.debug(log)
    
    schedule.every().day.at(scheduledTime).do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
    
    log = "[PROGRAM]     : " + "Stopping Program."
    print log
    logger.info(log)

    return


if __name__ == '__main__':
    main()

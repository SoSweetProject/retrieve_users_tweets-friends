#!/usr/bin/python3.5

import multiprocessing as mp
import persistqueue
import argparse
import sqlite3
import logging
import inspect
import shelve
import tweepy
import ujson
import time
import imp
import re

# this script collects tweets from each user continuously

logger = logging.getLogger(__name__)

consumer_key = 'oaJdaj6xWQVcpMgBJC0VA'
consumer_secret = '2zAfnaIDbGO6vXesL3omTl6N9pOOpvkZZDsVyGgfM0'

class TweetsFetcher(mp.Process):
    def __init__(
            self, name, access_token_key, access_token_secret, usersToTreatQueue, retrievedTweetsQueue, usersTreatedQueue):
        mp.Process.__init__(self)
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)
        self.twitter = tweepy.API(auth, wait_on_rate_limit=True)
        self.user = None
        self.usersToTreatQueue = usersToTreatQueue
        self.retrievedTweetsQueue = retrievedTweetsQueue
        self.usersTreatedQueue = usersTreatedQueue
        self.__stop = False
        self.name = name
        self.tweets = []
        logger.info("Worker %s created." % self.name)

    def __fetchUserTweets(self) :
        if not self.user :
            raise Exception
        self.tweets=[]
        try :
            statuses = tweepy.Cursor(self.twitter.user_timeline, user_id=self.user['id'], tweet_mode="extended", include_rts=False, since_id=self.user['id_last_tweet']).items()
            most_recent_tweet = False
            tweets = []
            for status in statuses :
                if most_recent_tweet is False :
                    id_last_tweet = ujson.dumps(status._json['id'])
                    most_recent_tweet = True
                tweets.append(ujson.dumps(status._json))
            self.tweets = tweets
        except tweepy.TweepError as e :
            if e.response is not None :
                logger.exception("Exception in %s (%s)"% (self.name, e.response.reason))
            else :
                logger.debug(e)
        except Exception as e :
            logger.exception("Exception in %s" % (self.name))
        finally :
            usersToTreat = getUsersToTreat()
            if self.tweets :
                self.retrievedTweetsQueue.put(self.tweets)
                self.user['id_last_tweet'] = int(id_last_tweet)
            user = {'id':self.user['id'],'screen_name':self.user['screen_name'],'id_last_tweet':self.user['id_last_tweet']}
            self.usersTreatedQueue.put(dict(user))
            logger.debug("%s has been put back in the queue", self.user['screen_name'])

    def run(self):
        logger.info("Starting worker %s"%self.name)
        while True :
            logger.info("%s is waiting a new user from queue" % (self.name))
            self.user = self.usersToTreatQueue.get()
            logger.info("%s is fetching tweets of %s (%d)" % (self.name, self.user['screen_name'], self.user['id']))
            self.__fetchUserTweets()
            if self.tweets :
                nTweets = len(self.tweets)
            else :
                nTweets = 0
            logger.info("%s is done with %s (%d). %d tweets collected." % (self.name, self.user['screen_name'], self.user['id'], nTweets))
        logger.info("%s is stopping." % (self.name))

class TweetsWriter(mp.Process):
    def __init__(self, path, retrievedTweetsQueue) :
        mp.Process.__init__(self)
        self.path = path
        self.retrievedTweetsQueue = retrievedTweetsQueue

    def run(self) :
        logger.info("Starting tweets writer")
        while True :
            userTweets = self.retrievedTweetsQueue.get()
            for status in userTweets :
                status=ujson.loads(status)
                folderPath = self.path+getFileName(status)
                file = open(folderPath,'a',buffering=1)
                file.write(ujson.dumps(status)+"\n")
                file.close()
            logger.info("writting %s (%s) finished" % (ujson.dumps(status['user']['screen_name']),ujson.dumps(status['user']['id'])))
            self.retrievedTweetsQueue.task_done()
        logger.info("Tweets writer has treated all users. Leaving.")

def parseArgs():
    parser = argparse.ArgumentParser(description="Fetch user's tweets from Twitter.")
    parser.add_argument('--database', '-d', required=True, help='path to the database')
    parser.add_argument('--folderPath', '-f', required=True, help='path to the folder that will contain the tweets')
    parser.add_argument('--log_level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    parser.add_argument('--log-file-path', '-P', default='.', help='log file path')
    return parser.parse_args()

def signal_handler(signum, frame):
    logger.critical('Signal handler called with signal %s' % str(signum))
    raise KeyboardInterrupt()

# This function opens a persist queue containing the users to be treated
def getUsersToTreat() :
    users = persistqueue.UniqueQ('/warehouse/COMPLEXNET/ltarrade/ressources/users_persistQueue', auto_commit=True)
    return users

# This function retrieves the workers and starts the collection of tweets
def createWorkers(cursor, usersToTreatQueue, retrievedTweetsQueue, usersTreatedQueue):
    cursor.execute('SELECT * FROM twitter_accounts')
    accounts = cursor.fetchall()
    logger.info("Using %d twitter accounts" % len(accounts))
    logger.info("creating workers...")
    workers = []
    for account in accounts:
        worker = TweetsFetcher(name=account['name'], access_token_key=account['access_token_key'], access_token_secret=account['access_token_secret'], usersToTreatQueue=usersToTreatQueue, retrievedTweetsQueue=retrievedTweetsQueue, usersTreatedQueue=usersTreatedQueue)
        workers.append(worker)
        worker.start()
    return workers

# This function converts the tweet's date into the correct format to name the file
def getFileName(status) :
    timeFormat = time.strftime('%Y-%m-%dT%H', time.strptime(status['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    fileName = timeFormat+".data"
    return fileName

# this function remove treated users from the persistent dictionary, put them back in the persistent queue and update user informations in database
def deleteAddUpdateUser(usersTreatedQueue, usersInProcess, usersToTreat, databaseName) :
    userTreated = usersTreatedQueue.get()
    usersInProcess = shelve.open("/warehouse/COMPLEXNET/ltarrade/ressources/usersInProcess", flag='c', protocol=None, writeback=False)
    del(usersInProcess[userTreated['screen_name']])
    usersInProcess.close()
    usersToTreat.put(dict(userTreated))
    connection = sqlite3.connect(databaseName)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    cursor.execute("UPDATE users SET id_last_tweet=? WHERE id=?", (userTreated['id_last_tweet'],userTreated['id']))
    connection.commit()
    connection.close()


def main() :
    # signal.signal(signal.SIGTERM, signal_handler)
    args = parseArgs()

    # logger setup
    if args.log_level == 'debug':
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.log_destination == 'file':
        handler = logging.FileHandler(args.log_file_path+'/'+inspect.getfile(inspect.currentframe()).split('/')[-1].split('.')[0]+'.log')
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
    logger.addHandler(handler)

    # Database connection
    connection = sqlite3.connect(args.database)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    databaseName = args.database

    # "usersToTreat" is a persistent queue containing all users to treat
    logger.debug("Retrieving users to be treated")
    usersToTreat = getUsersToTreat()
    logger.info("%d users to treat" % len(usersToTreat))

    # "usersToTreatQueue" will be the queue for users to treat, "retrievedTweetsQueue" will be the queue for tweets to write and "usersTreatedQueue" will be the queue for treated users
    usersToTreatQueue = mp.JoinableQueue()
    retrievedTweetsQueue = mp.JoinableQueue()
    usersTreatedQueue = mp.JoinableQueue()

    workers = createWorkers(cursor, usersToTreatQueue, retrievedTweetsQueue, usersTreatedQueue)

    connection.close()

    # if there has been an unexpected interruption in collection
    # "usersInProcess" is a persistent dictionary containing all users who are being processed
    usersInProcess = shelve.open("/warehouse/COMPLEXNET/ltarrade/ressources/usersInProcess", flag='c', protocol=None, writeback=False)
    numberOfUsersToTreat = len(usersInProcess)
    logger.info("(unexpected interruption) %d users to treat as a priority" % len(usersInProcess))
    interruptedProcess = False
    for user in usersInProcess :
        interruptedProcess = True
        usersToTreatQueue.put(dict(usersInProcess[user]))

    usersInProcess.close()

    try :
        writer = TweetsWriter(args.folderPath, retrievedTweetsQueue)
        writer.start()
        while True :
            # To first, users who were being processed at the time of the interruption are treated
            if interruptedProcess is True :
                for i in range(numberOfUsersToTreat) :
                    deleteAddUpdateUser(usersTreatedQueue, usersInProcess, usersToTreat, databaseName)
                interruptedProcess = False
                logger.info("Users to treat in priority have been treated")
            retrievedTweetsQueue.join()
            # From the persistent queue, users are put in the queue of users to process and in the persistent dictionary. Once processed, they are removed from the dictionary and put back in the persistent queue. They are treated in packs of 100.
            for i in range(100) :
                user = usersToTreat.get()
                usersToTreatQueue.put(dict(user))
                usersInProcess = shelve.open("/warehouse/COMPLEXNET/ltarrade/ressources/usersInProcess", flag='c', protocol=None, writeback=False)
                usersInProcess[user['screen_name']] = dict(user)
                usersInProcess.close()
            for i in range(100) :
                deleteAddUpdateUser(usersTreatedQueue, usersInProcess, usersToTreat, databaseName)
            retrievedTweetsQueue.join()
            logger.info("100 users were treated.")
    except KeyboardInterrupt :
        logger.warning("Script interupted. Terminating.")
        logger.warning("Killing tweets fetchers")
        for worker in workers:
            worker.join()
        for worker in workers:
            worker.terminate()
        while not usersTreatedQueue.empty() :
            deleteAddUpdateUser(usersTreatedQueue, usersInProcess, usersToTreat, databaseName)
        logger.warning("All workers killed")
        logger.warning("waiting for tweets writer...")
        retrievedTweetsQueue.join()
        logger.warning("users inserted.")
    except Exception as e :
        logger.exception("Exception : %s" % (str(e)))
    finally:
        logger.info("done.")

if __name__ == '__main__' :
    main()

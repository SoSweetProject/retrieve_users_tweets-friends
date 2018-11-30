#!/usr/bin/python

import tweepy
import ujson
import sqlite3
import datetime
import logging
import time
import multiprocessing as mp
import sys
import signal
import argparse
import inspect
import os
import re
import shutil

# This script goes through the user database and collect friendship for new users

logger = logging.getLogger(__name__)

consumer_key = 'oaJdaj6xWQVcpMgBJC0VA'
consumer_secret = '2zAfnaIDbGO6vXesL3omTl6N9pOOpvkZZDsVyGgfM0'

class FriendsWriter(mp.Process):
    def __init__(self,databaseName, usersTreatedQueue):
        mp.Process.__init__(self)
        self.connection = sqlite3.connect(databaseName)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.usersTreatedQueue=usersTreatedQueue

    def run(self):
        logger.info("Starting friend writer")
        while True:
            user = self.usersTreatedQueue.get()
            if user is None:
                self.usersTreatedQueue.task_done()
                break
            date = datetime.datetime.utcnow().isoformat()
            self.cursor.execute('UPDATE users SET date=?, friends=? WHERE id=?', (date, ujson.dumps(user['friends']), user['id']))
            logger.info("writting %s"% user['screen_name'])
            self.connection.commit()
            self.usersTreatedQueue.task_done()
        self.connection.close()
        logger.info("friend writer has treated all users. Leaving.")


class FriendsFetcher(mp.Process):
    def __init__(
            self, name, access_token_key, access_token_secret, usersToTreatQueue, usersTreatedQueue):
        mp.Process.__init__(self)
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token_key, access_token_secret)
        self.twitter = tweepy.API(auth)
        self.user = None
        self.usersToTreatQueue = usersToTreatQueue
        self.usersTreatedQueue = usersTreatedQueue
        self.__stop = False
        self.name = name
        logger.info("Worker %s created" % self.name)

    def __fetchUserFriends(self):
        if not self.user:
            raise Exception
        try:
            pages = tweepy.Cursor(
                self.twitter.friends_ids,
                user_id=self.user['id'],
                count=5000).pages()
            friends = []
            self.__waitUntilNextRequest()
            for page in pages:
                for friend in page:
                    friends.append(friend)
                self.__waitUntilNextRequest()
            self.user['friends'] = friends
        except tweepy.TweepError as e:
            if  e.response is not None:
                logger.exception("Exception in %s (%s)"% (self.name, e.response.reason))
                self.user['friends'] = e.response.reason
                logger.debug(self.user)
            else:
                 logger.debug(e)
        except Exception as e:
            logger.exception("Exception in %s" % (self.name))
        finally:
            if 'friends' in self.user:
                self.usersTreatedQueue.put(self.user)

    def __waitUntilNextRequest(self):
        status = self.twitter.rate_limit_status()['resources']['friends']['/friends/ids']
        if status['remaining'] < 1:
            t = status['reset'] - time.time() + 10  # we wait 10 more seconds to be sure
            logger.info("%s is sleeping for %.0f minutes and %.0f seconds" % ((self.name,) + divmod(t, 60)))
            time.sleep(t)
            logger.info("%s is waking up" % (self.name))

    def run(self):
        logger.info("Starting worker %s"%self.name)
        while True:
            self.user = self.usersToTreatQueue.get()
            if self.user is None:
                logger.debug("%s's user is None"%self.name)
                self.usersToTreatQueue.task_done()
                break
            logger.info("%s is fetching friends of %s (%d)" % (self.name, self.user['screen_name'], self.user['id']))
            self.__fetchUserFriends()
            self.usersToTreatQueue.task_done()
            if ('friends' in self.user) and (self.user['friends'] is not None) :
                nFriends=len(self.user['friends'])
            else :
                nFriends=0
            logger.info("%s is done with %s (%d). %d friends collected." % (self.name, self.user['screen_name'], self.user['id'],nFriends))
        logger.info("%s is stopping." % (self.name))


def signal_handler(signum, frame):
    logger.critical('Signal handler called with signal %s' % str(signum))
    raise KeyboardInterrupt()


def parseArgs():
    parser = argparse.ArgumentParser(description="Fetch users' friends from Twitter.")
    parser.add_argument('--r_database', '-r', required=True, help='path to the database file that will be opened read-only. If the path specified in --rw_database doesn\'t exist, the database will be created from this one')
    parser.add_argument('--rw_database', '-w', required=True, help='path to the database file that will be opened for reading and writing. If the path already exists, the argument --r_database is ignored')
    parser.add_argument('--log_level', '-l', default='info', help='logger verbosity level', choices=['info', 'debug'])
    parser.add_argument('--log-destination', '-L', default='file', help='logger destination', choices=['file', 'stderr'])
    parser.add_argument('--log-file-path', '-P', default='.', help='log file path')
    return parser.parse_args()


def createWorkers(cursor, usersToTreatQueue, usersTreatedQueue):
    cursor.execute('SELECT * FROM twitter_accounts')
    accounts = cursor.fetchall()
    logger.info("Using %d twitter accounts" % len(accounts))
    logger.info("creating workers...")
    workers = []
    for account in accounts:
        worker = FriendsFetcher(name=account['name'], access_token_key=account['access_token_key'], access_token_secret=account['access_token_secret'], usersToTreatQueue=usersToTreatQueue, usersTreatedQueue=usersTreatedQueue)
        workers.append(worker)
        worker.start()
    return workers

def getUsersToTreat(cursor):
    cursor.execute('SELECT * FROM users where friends is NULL')
    users = cursor.fetchall()
    return users

def createDatabase(): # creates a new database which is a copy of --database, but only the 'id' and 'screen_name' columns of 'users' are filled in.
    args = parseArgs()
    database_path = args.rw_database
    database_name = os.path.basename(database_path)
    # Copies the contents of the old database to the new one
    database = sqlite3.connect(args.r_database)
    logger.info("start of database copy")
    shutil.copyfile(args.r_database,args.rw_database)
    logger.info("database %s created" %database_name)
    new_database = sqlite3.connect(args.rw_database)
    #  Empties the 'date' and 'friends' columns and fills with a NULL value
    new_database.row_factory = sqlite3.Row
    cursor = new_database.cursor()
    cursor.execute('UPDATE users SET friends=NULL, date=NULL')
    logger.info('new database %s is created' % database_name)
    new_database.commit()
    return database_path

def main():
    # signal.signal(signal.SIGTERM, signal_handler)
    args = parseArgs()

    #Logger setup
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

    #Open the sqlite database
    if not os.path.exists(args.rw_database) :
        new_database = createDatabase()
        logger.info("Opening database %s" % new_database)
        connection = sqlite3.connect(new_database)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        current_database = new_database
    else :
        logger.info("Opening database %s" % args.rw_database)
        connection = sqlite3.connect(args.rw_database)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        current_database = args.rw_database

    usersToTreatQueue = mp.JoinableQueue()
    usersTreatedQueue = mp.JoinableQueue()

    logger.debug("Retrieving users to be treated")
    usersToTreat = getUsersToTreat(cursor)
    logger.info("%d users to treat" % len(usersToTreat))
    workers = createWorkers(cursor, usersToTreatQueue, usersTreatedQueue)
    connection.close()
    try:
        for user in usersToTreat:
            usersToTreatQueue.put(dict(user))
        for worker in workers:
            usersToTreatQueue.put(None)
        writer = FriendsWriter(current_database, usersTreatedQueue)
        writer.start()
        usersToTreatQueue.join()
        logger.debug("Users to be treated queue is empty")
        usersTreatedQueue.put(None)
        usersTreatedQueue.join()
        logger.debug("treated users queue is empty")
    except KeyboardInterrupt:
        logger.warning("Script interupted. Terminating.")
        logger.warning("Killing friends fetchers")
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.join()
        logger.warning("All workers killed")
        logger.warning("waiting for friend writer...")
        usersTreatedQueue.put(None)
        usersTreatedQueue.join()
        logger.warning("users inserted.")
    finally:
        logger.info("done.")

if __name__ == '__main__':
    main()

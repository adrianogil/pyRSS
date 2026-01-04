#!/usr/bin/python

# Based on https://alvinalexander.com/python/python-script-read-rss-feeds-database  

import argparse
import feedparser
import time
from subprocess import check_output
import sys
import os

#feed_name = 'TRIBUNE'
#url = 'http://chicagotribune.feedsportal.com/c/34253/f/622872/index.rss'

parser = argparse.ArgumentParser(description='Fetch RSS feeds and store post timestamps.')
parser.add_argument('feed_name', help='Name of the feed')
parser.add_argument('url', nargs='?', help='RSS feed URL')
parser.add_argument('--delete-feed', action='store_true', help='Delete feed entries from the database')
args = parser.parse_args()

feed_name = args.feed_name
url = args.url

db = 'data/feeds.db'
limit = 12 * 3600 * 1000

#
# function to get the current time
#
current_time_millis = lambda: int(round(time.time() * 1000))
current_timestamp = current_time_millis()

def parse_db_line(line):
    parts = line.rstrip('\n').split('|')
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, line.rstrip('\n'), None

def post_is_in_db(title):
    with open(db, 'r') as database:
        for line in database:
            line_feed, line_title, _ = parse_db_line(line)
            if line_title == title and (line_feed is None or line_feed == feed_name):
                return True
    return False

# return true if the title is in the database with a timestamp > limit
def post_is_in_db_with_old_timestamp(title):
    with open(db, 'r') as database:
        for line in database:
            line_feed, line_title, ts_as_string = parse_db_line(line)
            if line_title == title and (line_feed is None or line_feed == feed_name):
                ts = long(ts_as_string)
                if current_timestamp - ts > limit:
                    return True
    return False

def delete_feed_entries():
    if not os.path.exists(db):
        print("Database not found: %s" % (db,))
        return
    with open(db, 'r') as database:
        lines = database.readlines()
    with open(db, 'w') as database:
        for line in lines:
            line_feed, _, _ = parse_db_line(line)
            if line_feed != feed_name:
                database.write(line)
    print("Deleted entries for feed: %s" % (feed_name,))

if args.delete_feed:
    delete_feed_entries()
    sys.exit(0)

if not url:
    print("Error: url is required unless --delete-feed is used.")
    sys.exit(1)

#
# get the feed data from the url
#
feed = feedparser.parse(url)

#
# figure out which posts to print
#
posts_to_print = []
posts_to_skip = []

for post in feed.entries:
    # if post is already in the database, skip it
    # TODO check the time
    title = post.title
    if post_is_in_db_with_old_timestamp(title):
        posts_to_skip.append(title)
    else:
        posts_to_print.append(title)
    
#
# add all the posts we're going to print to the database with the current timestamp
# (but only if they're not already in there)
#
f = open(db, 'a')
for title in posts_to_print:
    if not post_is_in_db(title):
        f.write(feed_name + "|" + title.encode('utf-8') + "|" + str(current_timestamp) + "\n")
f.close
    
#
# output all of the new posts
#
count = 1
blockcount = 1
for title in posts_to_print:
    if count % 5 == 1:
        print("\n" + time.strftime("%a, %b %d %I:%M %p") + '  ((( ' + feed_name + ' - ' + str(blockcount) + ' )))')
        print("-----------------------------------------\n")
        blockcount += 1
    print(title + "\n")
    count += 1

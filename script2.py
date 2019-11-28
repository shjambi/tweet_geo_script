import json
import pprint
import sys
import time
import re


count = 0
total = 0

def stream_read_json(json_file):  
    with open(json_file, 'r') as f:
        for jsonline in f:
            yield json.loads(jsonline)

def update_tweet_object(tweet, level=0, attr1="", attr2="", attr3=""):
    global count
    if level == 0:
        if tweet['place'] != None:
                tweet['place']['bounding_box']="{}".format(tweet['place']['bounding_box'])
                count += 1
                # print(tweet['id'])
                # print("---Total tweets updated: %s" % count) 
                # sys.stdout.flush()
                print(tweet["id"], count)
                # sys.stdout.write("\rTotal processed tweets: %s" % count)
                sys.stdout.flush()

    elif level == 1:
            if tweet[attr1]['place'] != None:
                tweet[attr1]['place']['bounding_box']="{}".format(tweet[attr1]['place']['bounding_box'])
                count += 1
                # print(tweet['id'])
                print(tweet["id"], count)
                # print("***Total tweets updated: %s" % count)                
                # sys.stdout.write("\rTotal processed tweets: %s" % count)
                sys.stdout.flush()
    elif level == 2:
            if tweet[attr1][attr2]['place'] != None:
                tweet[attr1][attr2]['place']['bounding_box']="{}".format(tweet[attr1][attr2]['place']['bounding_box'])
                count += 1
                # print(tweet['id'])
                print(tweet["id"], count)
                # print("===Total tweets updated: %s" % count) 
                # sys.stdout.flush()
                # sys.stdout.write("\rTotal processed tweets: %s" % count) 
                sys.stdout.flush()
    return tweet    

def update_tweet_json(file_path):
    global count
    for tweet in stream_read_json(file_path):

        tweet = update_tweet_object(tweet, 0)
        if 'quoted_status' in tweet:
            tweet = update_tweet_object(tweet, 1, 'quoted_status')
            if 'quoted_status' in tweet['quoted_status']:
                tweet = update_tweet_object(tweet, 2, 'quoted_status', 'quoted_status')
                print("Found quoted_status in quoted_status")
            if 'retweeted_status' in tweet['quoted_status']:
                tweet = update_tweet_object(tweet, 2, 'quoted_status', 'retweeted_status')  
                print("Found retweeted_status in quoted_status")    
        if 'retweeted_status' in tweet:
            tweet = update_tweet_object(tweet, 1, 'retweeted_status')
            # if 'retweeted_status' in tweet['retweeted_status']:
            #     tweet = update_tweet_object(tweet, 2, 'retweeted_status', 'retweeted_status')
            if 'quoted_status' in tweet['retweeted_status']:
                tweet = update_tweet_object(tweet, 2, 'retweeted_status', 'quoted_status')

        yield tweet

def write_json(original_file_path, result_file_path):
    global total
    # print("Processing tweets...")
    with open(result_file_path, 'w') as f:
        for tweet in update_tweet_json(original_file_path):
            total += 1
            f.write(json.dumps(tweet))
            f.write('\n')
            # count += 1
            # sys.stdout.write("\rTotal tweets processed: %s" % count)
            # sys.stdout.flush()

    # print("\ncount: %s" % count)
    
    
if __name__ == '__main__':
    
    original_file_path = "/Users/projectepic/Documents/winter_tweet_1555026252910_1000.json"
    result_file_path = "/Users/projectepic/Documents/winter_tweet_1555026252910_1000_new.json"
 
    start_time = time.time()
    write_json(original_file_path, result_file_path)
    end_time = time.time() - start_time
    print('\nTotal number of tweets: %s' % total)
    print('Processing time: %s seconds' % end_time)
    print("Processing done!")
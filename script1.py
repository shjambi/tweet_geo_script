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

def update_tweet_json(file_path):
    global count
    for tweet in stream_read_json(file_path):        
        
        if tweet['place'] != None:
            tweet['place']['bounding_box']="{}".format(tweet['place']['bounding_box'])
            # pprint.pprint(tweet['place'])
            count += 1
            print(tweet['id'])
            print("-------Total tweets updated: %s" % count)

        if 'quoted_status' in tweet:
            if tweet['quoted_status']['place'] != None:
                tweet['quoted_status']['place']['bounding_box']="{}".format(tweet['quoted_status']['place']['bounding_box'])
                count += 1
                print(tweet['id'])
                print("+++++++Total tweets updated: %s" % count)
  
        if 'retweeted_status' in tweet:
            if tweet['retweeted_status']['place'] != None:
                tweet['retweeted_status']['place']['bounding_box']="{}".format(tweet['retweeted_status']['place']['bounding_box'])
                count += 1
                print(tweet['id'])
                print("&&&&&&&Total tweets updated: %s" % count)
        
            if 'quoted_status' in tweet['retweeted_status']:
                if tweet['retweeted_status']['quoted_status']['place'] != None:
                    tweet['retweeted_status']['quoted_status']['place']['bounding_box']="{}".format(tweet['retweeted_status']['quoted_status']['place']['bounding_box'])
                    count += 1
                    print(tweet['id'])
                    print(",,,,,,,Total tweets updated: %s" % count)

            if 'extended_tweet' in tweet['retweeted_status']:
                if 'place' in tweet['retweeted_status']['extended_tweet']:
                    print("FOUND place in extended_tweet")
        yield tweet

def write_json(original_file_path, result_file_path):
    # count = 0
    global total
    with open(result_file_path, 'w') as f:
        for tweet in update_tweet_json(original_file_path):
            total += 1
            f.write(json.dumps(tweet))
            f.write('\n')
            # count += 1
            # sys.stdout.write("\rTotal tweets processed: %s" % count)
            # sys.stdout.flush()

if __name__ == '__main__':   
    original_file_path =  "/Users/projectepic/Documents/winter_tweet_1555022320703_1000.json"
    result_file_path = "/Users/projectepic/Documents/winter_tweet_1555022320703_1000_new.json"
    start_time = time.time()
    write_json(original_file_path, result_file_path)
    end_time = time.time() - start_time
    print('\nTotal number of tweets: %s' % total)
    print('\nTotal processing time: %s seconds' % end_time)
    print("\nProcessing Done!")
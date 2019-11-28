import json
import pprint
import sys
import time
import re
from collections import OrderedDict

count = 0
total = 0

def stream_read_json(json_file):  
    with open(json_file, 'r') as f:
        for jsonline in f:
            yield json.loads(jsonline.decode('utf-8'), object_pairs_hook=OrderedDict)

def boil_down_nested(tweet):
    # result = {}
    attr_list = []
    for k, v in tweet.items():
        if k == "b" and v is not None: attr_list.append((k, "{}".format(v)))
                # result[k] = "{}".format(v)
        elif k in ["a", "c"]: # and "b" in v:
                # result.update(k=boil_down_nested(v))
                # result[k] = boil_down_nested(v)
                attr_list.append((k, boil_down_nested(v)))
        else:
            attr_list.append((k, v))
            # result[k] = v
        print(attr_list)
    return OrderedDict(attr_list) 

def update_tweet_json(file_path):
    global count
    for tweet in stream_read_json(file_path): 
        yield boil_down_nested(tweet)

def write_json(original_file_path, result_file_path):
    global total
    # print("Processing tweets...")
    with open(result_file_path, 'w') as f:
        for tweet in update_tweet_json(original_file_path):
            total += 1
            f.write(json.dumps(tweet))
            f.write('\n')
    
if __name__ == '__main__':
    original_file_path =  "./t1.json"
    result_file_path =  "./t2.json"
    start_time = time.time()
    write_json(original_file_path, result_file_path)
    end_time = time.time() - start_time
    print('\nTotal number of tweets: %s' % total)
    print('Processing time: %s seconds' % end_time)
    print("Processing done!")) 
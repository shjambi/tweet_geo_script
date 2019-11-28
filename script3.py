import json
import sys, argparse, time, os
from collections import OrderedDict

count = 0
total = 0

def parse_nested_json(tweet, id, depth):
    '''
    A recursive function that reads a tweet object and its nested tweet objects, 
    stringifies any occurances of place/bounding_box attribute, then
    returns a new dictionary of the whole nested tweet object.
    '''
    # To preserve a tweet's attributes order while parsing it recursively, 
    # a list of tuples is used to construct a new updated tweet json.
    # Then the temporary list is converted into a dictionary object.
    attr_list = []  

    global count
    for key, value in tweet.items():       
        if key == "place" and value is not None:       
            value["bounding_box"] = "{}".format(dict(value["bounding_box"]))
            attr_list.append((key, value))
            count += 1
            sys.stdout.write("\rTotal processed tweets: %s" % count) 
            sys.stdout.flush()
            # print(" (id = %s d = %s)" % (id, depth))
        elif key in ["retweeted_status", "quoted_status"]:
            depth += 1
            attr_list.append((key, parse_nested_json(value, id, depth)))
        else:
            attr_list.append((key, value))
    return OrderedDict(attr_list)  

def stream_read_json(json_file):  
    with open(json_file, 'r') as input_json:
        for json_line in input_json:
            yield json.loads(json_line.decode('utf-8'), object_pairs_hook=OrderedDict)

def update_tweet_object(json_file):
    for tweet in stream_read_json(json_file):
       yield parse_nested_json(tweet, tweet["id"], 0)

def stream_write_json(input_file, output_file):
    global total
    with open(output_file, 'w') as output_json:
        for tweet in update_tweet_object(input_file):
            total += 1
            output_json.write(json.dumps(tweet))
            output_json.write('\n')
        output_json.seek(-1, os.SEEK_END)
        output_json.truncate()

def main(input_file, output_file):
    start_time = time.time()
    stream_write_json(input_file, output_file)
    end_time = time.time() - start_time
    print('\nTotal number of tweets: %s' % total)
    print('Total processing time: %s seconds' % round(end_time,2))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'To run the script file: script.py -i <inputfile> -o <outputfile>')
    parser.add_argument("--inputfile", "-i", help="Set input file")
    parser.add_argument("--outputfile", "-o", help="Set output file")
    args = parser.parse_args()
    if args.inputfile and args.outputfile:
        main(args.inputfile, args.outputfile)
    else:
        print("Run script.py -h (or --hrlp) for help")
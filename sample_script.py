# Authors: Mazin Hakeem & Ahmet Arif Aydin
# File name: json_extractor.py
# Version: 1.1
# Date: April 12, 2013
# Description: Extract JSON (Tweet) objects from 'catalina.out' and dump them to 'results.json'
# Also, create a log file with the output file info.

import re
import os
import time
import sys
import humanifier


class BigFile(object):
    '''This class is for reading large size files (in Gigabytes and more)
    line by line rather than loading the whole file to memory. It can also be
    used for smaller files. Must pass file path while creating a class object.'''

    def __init__(self, file_path):
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)

    def read_line(self):
        '''Read the current line only in the file, and delete the old line after
        each iteration.'''
        with open(self.file_path, 'r') as file_object:
            for line in file_object:
                yield line

    def get_file_size(self):
        '''Return the file size.'''
        return self.file_size


class JSONExtractor(object):
    '''This class extracts JSON objects from the big file that is being read,
    and then save them in a .json file.'''

    def __init__(self, big_file):
        self.big_file = big_file
        # JSON objects counter
        self.json_count = 0
        # Size of the current data being read from the file
        self.file_read_size = 0.0

    def get_json_count(self):
        '''Return JSON counter.'''
        return self.json_count

    def count_json(self):
        '''Increment the JSON counter by adding 1.'''
        self.json_count += 1

    def extract_json(self):
        '''Extract new JSON objects from the current line if from a given
        search term, and do not store the old JSON in memory.'''
        for line in self.big_file.read_line():
            # Increment the file read size after reading each line
            self.file_read_size += len(line)
            # Show file processing progress on screen while reading lines
            self.show_progress()

            # Search for the given term in the current line
            match = re.search('314 - ', line)
            # If the line has that term, then it contains a JSON object
            if match:
                json = line.strip().split('314 - ')[1] + '\n'
                self.count_json()
                yield json

    def write_to_file(self, file_path):
        '''Write the JSON objects to a file.

        Keyword arguments:
        file_path -- the desired file destination

        '''
        with open(file_path, 'w') as file_object:
            for json in self.extract_json():
                file_object.write(json)

    def show_progress(self):
        '''Show JSON extraction progress in percentage on the terminal screen.'''
        total_file_size = self.big_file.get_file_size()
        sys.stdout.write('\rProcessed: %.2f%%' % ((self.file_read_size/total_file_size)*100))
        sys.stdout.flush()
        # This line is to check whether the progress is working. Uncomment when needed to test ONLY.
        # time.sleep(0.01)

if __name__ == '__main__':

    start_time = time.time()
    print '*'*50
    print "The file is being processed..."

    big_file = BigFile('sample.txt')
    json = JSONExtractor(big_file)

    print 'The file is writing... \nPlease wait...'
    json.write_to_file('results.json')

    end_time = time.time() - start_time
    print "\nProcessing Done!"
    tweet_count = humanifier.millify(json.get_json_count())
    print 'Total no. of JSON (Tweet) objects: %s' % tweet_count
    print 'Total processing time: %s seconds.' % end_time
    print '*'*50

    # Write to log file
    with open('results.json.log', 'w') as file_object:
        file_object.write('results.json info:\n')
        file_object.write('%s Tweets\n' % tweet_count)
        file_object.write('File size: %s' % humanifier.bytify(os.path.getsize('results.json')))
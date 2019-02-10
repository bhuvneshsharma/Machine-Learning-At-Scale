#!/usr/bin/env python
"""
Reads output from mapper.py and prints out word \t 1 if not equal to previous word
INPUT:
    word \t 1
OUTPUT:
    word \t 1 for word not equal to previous word
"""

#import re
import sys

cur_word = None
word_count = 0

# read from standard input
for line in sys.stdin:
    #line = line.strip()

############ YOUR CODE HERE #########
    key, value = line.split()
    # tally counts from current key
    if key != cur_word: 
        print '%s\t%s' % (key, value)
    cur_word, word_count  = key, int(value)

############ (END) YOUR CODE #########
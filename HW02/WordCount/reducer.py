#!/usr/bin/env python
"""    
Reads output from mapper.py and counts frequency for each word. Note to count correctly input must be sorted or if below won't lead to complete count. 
INPUT:
    word \t 1
OUTPUT:
    word \t count
USAGE:
    python reducer.py < output of mapper.py
"""

import sys

# initialize trackers
cur_word = None
cur_count = 0

# read input key-value pairs from standard input
for line in sys.stdin:
    # split word \ t into key and value
    key, value = line.split()
    # tally counts from current key
    if key == cur_word: 
        cur_count += int(value)
    # OR emit current total and start a new tally 
    else: 
        if cur_word:
            print '%s\t%s' % (cur_word, cur_count)
        cur_word, cur_count  = key, int(value)

# don't forget the last record! 
print '%s\t%s' % (cur_word, cur_count)
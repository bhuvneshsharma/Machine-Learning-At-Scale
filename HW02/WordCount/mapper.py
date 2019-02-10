#!/usr/bin/env python
"""
Reads stdin line by line and prints for each word, word \t 1
INPUT:
    text file
OUTPUT:
    word \t 1
USAGE:
    python mapper.py < textfile
"""

import re
import sys

# read from standard input
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # tokenize - split on whitespace, filter out non-alpha, change to lowercase
    words = re.findall(r'[a-z]+', line.lower())
    # emit words and count of 1
    for word in words:
        print '%s\t%s' % (word, 1)
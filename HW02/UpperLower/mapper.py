#!/usr/bin/env python
"""
Read from stdin and process each word, if it has uppercase, print upper \ 1, otherwise lower \t 1
INPUT:
    textfile
OUTPUT:
    case [upper or lower] \t 1 
"""
import re
import sys

# read from standard input
for line in sys.stdin:
    
    ############ YOUR CODE HERE #########

    # tokenize words
    words = re.findall(r'[A-Za-z]+', line.strip())
    # look at each word, if it has upper case print upper 1, otherwise lower 1
    for word in words:
        if any (c.isupper() for c in word):
            print '%s\t%s' % ('upper', 1)
        else:
            print '%s\t%s' % ('lower', 1)
    
    
    
    
    
    ############ (END) YOUR CODE #########
#!/usr/bin/env python
"""
Mapper tokenizes and emits words with their class.
INPUT:
    word \t class \t partialCount 
OUTPUT:
    word \t class \t totalCount  
"""
import re
import sys

# initialize trackers
cur_word = None
spam_count, ham_count = 0,0

# read from standard input
for line in sys.stdin:
    # parse input
    word, isSpam, count = line.split('\t')
    
############ YOUR CODE HERE #########
    
    # Note: Solution assumes Hadoop sorts intermediate d
    if word == cur_word:
        if isSpam == '1':
            spam_count += int(count)
        else:
            ham_count += int(count)
    else:
        if cur_word:
            print "%s\t%s\t%s" % (cur_word, 1, spam_count)
            print "%s\t%s\t%s" % (cur_word, 0, ham_count)

        spam_count, ham_count = 0,0

        if isSpam == '1':
            spam_count = int(count)
        else:
            ham_count = int(count)

    cur_word = word

# final line
if isSpam == 1:
    print "%s\t%s\t%s" % (word, isSpam, spam_count)
else:
    print "%s\t%s\t%s" % (word, isSpam, ham_count)

        

############ (END) YOUR CODE #########
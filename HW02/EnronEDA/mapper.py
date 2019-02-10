#!/usr/bin/env python
"""
Mapper tokenizes and emits words with their class.
INPUT:
    ID \t SPAM \t SUBJECT \t CONTENT \n
OUTPUT:
    word \t class \t count 
"""
import re
import sys
from collections import defaultdict # Use dict but note memory issues

# read from standard input
for line in sys.stdin:
    # parse input
    docID, _class, subject, body = line.split('\t')
    # tokenize
    words = re.findall(r'[a-z]+', subject + ' ' + body)
############ YOUR CODE HERE #########

    counts = defaultdict(int) # define dict (one for each record)

    for word in words:
        counts[word] += 1

    for i in counts:
        print '%s\t%s\t%s' % (i, _class, counts[i])


############ (END) YOUR CODE #########

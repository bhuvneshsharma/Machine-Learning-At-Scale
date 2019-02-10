#!/usr/bin/env python
"""
Mapper reads in text documents and emits word counts by class.
INPUT:
    ID \t SPAM \t SUBJECT \t CONTENT \n
OUTPUT:
    word \t class \t count 
    
Instructions:
    You know what this script should do, go for it!
    (As a favor to the graders, please comment your code clearly!)
    
    A few reminders:
    1) To make sure your results match ours please be sure
       to use the same tokenizing that we have provided in
       all the other jobs:
         words = re.findall(r'[a-z]+', text-to-tokenize.lower())
         
    2) Don't forget to handle the various "totals" that you need
       for your conditional probabilities and class priors.
"""
##################### YOUR CODE HERE ####################

import re
import sys
from collections import defaultdict # Use dict but note memory issues

# read from standard input

# counter for totals
ham_counts, spam_counts = 0, 0
partial_ham_counts, partial_spam_counts = 0, 0

# list of distinct word, trying to reduce memory use by not using universal dict; used for part e)
listword =[]

for line in sys.stdin:

    # parse input
    docID, _class, subject, body = line.split('\t')
    
    # tokenize
    words = re.findall(r'[a-z]+', subject.lower() + ' ' + body.lower())

    # save info in a dict
    counts = defaultdict(int) # define dict (one for each record to minimize memory issues)

    for word in words:
        counts[word] += 1

    for i in counts:
        print '%s\t%s\t%s' % (i, _class, counts[i])
        listword.append(i) # REQUIRED FOR LATER LAP SMOOTHING

    if _class == '1': 
        spam_counts += 1
        partial_spam_counts += sum(counts.values())
    else:
        ham_counts += 1
        partial_ham_counts += sum(counts.values())
        
# LAPLACE create a string to print as count and to be read by reducer        
listword = ','.join(str(i) for i in listword)
        
print '%s\t%s\t%s' % ('0ClassPriors', 1, spam_counts)
print '%s\t%s\t%s' % ('0ClassPriors', 0, ham_counts)
print '%s\t%s\t%s' % ('1PartialCounts', 1, partial_spam_counts)
print '%s\t%s\t%s' % ('1PartialCounts', 0, partial_ham_counts)
print '%s\t%s\t%s' % ('1VocabSize', 1, listword)

##################### (END) YOUR CODE #####################
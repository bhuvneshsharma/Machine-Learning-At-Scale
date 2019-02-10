#!/usr/bin/env python
"""
This script reads word counts from STDIN and combines
the counts for any duplicated words.

INPUT & OUTPUT FORMAT:
    word \t count
USAGE:
    python collateCounts.py < yourCountsFile.txt

Instructions:
    For Q6 - Use the provided code as is.
    For Q7 - Delete the section marked "PROVIDED CODE" and replace
             it with your own implementation. Your solution should
             not use a dictionary or store counts in any way - just
             print them as soon as you've added them. HINT: you've
             modified the framework script to ensure that the input
             is alphabetized; how can you use that to your advantage?
"""

# imports
import sys
from collections import defaultdict

########### PROVIDED IMPLEMENTATION ##############
#counts = defaultdict(int)
# stream over lines from Standard Input
#for line in sys.stdin:
    # extract words & counts
#    word, count  = line.split()
    # tally counts
#    counts[word] += int(count)
# print counts
#for wrd, count in counts.items():
#    print("{}\t{}".format(wrd,count))
########## (END) PROVIDED IMPLEMENTATION #########

################# YOUR CODE HERE #################

previous_word = []
previous_count = []
for line in sys.stdin:
    word, count = line.split()
    if word == previous_word:
        count = int(count) + int(previous_count)
    if previous_word != [] and word != previous_word:
        print("{}\t{}".format(previous_word,previous_count))
    previous_word = word
    previous_count = count


        







################ (END) YOUR CODE #################

#!/usr/bin/env python
# START STUDENT CODE HW32CFREQREDUCER

import sys
sys.stderr.write("reporter:counter:Combiner Counters,Calls,1\n")

# initialize trackers
cur_word = None
total_counts = 0

# read from standard input
for line in sys.stdin:
    # parse input
    count, word = line.split()
    
    if word == '0total':
        total_counts += int(count)
        if word != cur_word:
            print "%s\t%s\t%s" % (word, cur_word, total_counts)

    # Now process rest
    
    if (word != '0total') and cur_word:
        print "%s\t%s\t%s" % (word, cur_word, count)
            
    cur_word = word
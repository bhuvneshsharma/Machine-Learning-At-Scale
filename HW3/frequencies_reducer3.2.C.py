#!/usr/bin/env python
# START STUDENT CODE HW32CFREQREDUCER

import sys
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

total = 0

# read from standard input
for line in sys.stdin:
    # parse input
    line = line.strip()
    word, count = line.split('\t')
    
    if word == '!total':
        total += int(count)

    else:
        print "%s\t%s\t%s" % (count, float(count)/float(total), word)
    

# END STUDENT CODE HW32CFREQREDUCER
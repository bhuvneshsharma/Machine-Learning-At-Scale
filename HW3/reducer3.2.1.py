#!/usr/bin/env python
# START STUDENT CODE HW32CFREQREDUCER

from __future__ import division
import sys
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

# initialize trackers
total = 0

# read from standard input
for line in sys.stdin:
    line = line.strip()
    partitionkey, value, key = line.split('\t')
    
    if key == '!total':
        total += int(value)
    else:
        print "%s\t%s\t%s" % (value, int(value)/total, key)
    

# END STUDENT CODE HW32CFREQREDUCER
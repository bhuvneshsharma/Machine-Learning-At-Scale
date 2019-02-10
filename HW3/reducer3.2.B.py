#!/usr/bin/env python
# START STUDENT CODE HW32BREDUCER

import sys
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

# initialize trackers
cur_issue = None
cur_count = 0

# read input key-value pairs from standard input
for line in sys.stdin:
    # split product \ t into key and value
    key, value = line.split('\t')
    if key == cur_issue: 
        cur_count += int(value)
    # OR emit current total and start a new tally 
    else: 
        if cur_issue:
            print '%s\t%s' % (cur_issue, cur_count)
        cur_issue, cur_count  = key, int(value)

# don't forget the last record! 
print '%s\t%s' % (cur_issue, cur_count)
    

# END STUDENT CODE HW32BREDUCER
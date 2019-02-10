#!/usr/bin/env python
# START STUDENT CODE HW31REDUCER

"""    
Reads output from mapper.py and counts frequency for each product. 
INPUT:
    product \t 1
OUTPUT:
    product \t count
USAGE:
    python complaintCountReducer.py < output of mapper.py
"""

import sys
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

# initialize trackers
cur_product = None
cur_count = 0

# read input key-value pairs from standard input
for line in sys.stdin:
    # split product \ t into key and value
    key, value = line.split('\t')
    if key == cur_product: 
        cur_count += int(value)
    # OR emit current total and start a new tally 
    else: 
        if cur_product:
            print '%s\t%s' % (cur_product, cur_count)
        cur_product, cur_count  = key, int(value)

# don't forget the last record! 
print '%s\t%s' % (cur_product, cur_count)
    
    
# END STUDENT CODE HW31REDUCER
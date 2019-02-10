#!/usr/bin/env python
# START STUDENT CODE HW32CFREQMAPPER

import sys
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

total = 0 
counts_median = 2508


# read from standard input
for line in sys.stdin:
    key, value = line.strip().split()
    total += int(value)
    
    partitionkey = 'b'
    if int(value) <= counts_median:
        partitionkey = 'a'
        
    print '%s\t%s\t%s' % (partitionkey, value, key)

print '%s\t%s\t%s' % ('a', total, '!total')
print '%s\t%s\t%s' % ('b', total, '!total')
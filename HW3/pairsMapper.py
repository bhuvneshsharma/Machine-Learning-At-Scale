#!/usr/bin/env python
from __future__ import division
import sys
import itertools

############################################################
# Using order inversion, we are able to count the total
# number of baskets to send to the reducer for calculating
# the relative frequencies.
############################################################


sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

total = 0

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    total += 1
    for subset in itertools.combinations(sorted(set(words)), 2):
        print '%s\t%s' % (subset[0]+" - "+subset[1], 1)
print "%s\t%s" % ("!TOTAL", total)    
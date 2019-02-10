#!/usr/bin/env python
from __future__ import division
import sys
import itertools
import json

############################################################
# Using order inversion, we are able to count the total
# number of baskets to send to the reducer for calculating
# the relative frequencies.
############################################################


sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

N_BASKETS = 0

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    N_BASKETS += 1
    H = {}
    for subset in itertools.combinations(sorted(set(words)), 2):
        if subset[0] not in H.keys():
            H[subset[0]] = {}
            H[subset[0]][subset[1]] = 1 
        elif subset[1] not in H[subset[0]]:
            H[subset[0]][subset[1]] = 1
        else:
            H[subset[0]][subset[1]] += 1 

    for key in H.keys():
        print "%s\t%s" % (key, json.dumps(H[key]))
            
            
print "%s\t%s" % ("!N_BASKETS", N_BASKETS)    
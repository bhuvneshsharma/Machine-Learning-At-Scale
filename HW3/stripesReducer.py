#!/usr/bin/env python
from __future__ import division
import sys
import collections as cl
import json

sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

N_BASKETS = 0
cur_key = None
cur_counter = cl.Counter()
support_count = 100

for line in sys.stdin:
    line = line.strip()
        
    key,value = line.split("\t")
    if key == "!N_BASKETS":
        N_BASKETS += int(value)
        
    elif key == cur_key:
        cur_counter.update(json.loads(value))
    else:
        if cur_key:
            for k in cur_counter.keys():
                print '%s\t%s\t%s' % (cur_key+" - "+k, cur_counter[k], str(cur_counter[k]/N_BASKETS))
        cur_key = key
        cur_counter = cl.Counter(json.loads(value))

for k in cur_counter.keys():
    print '%s\t%s\t%s' % (cur_key+" - "+k, cur_counter[k], str(cur_counter[k]/N_BASKETS))
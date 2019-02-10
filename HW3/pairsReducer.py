#!/usr/bin/env python
from __future__ import division
import sys

sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

total = 0
cur_key = None
cur_count = 0
support_count = 100

for line in sys.stdin:
    line = line.strip()
    key,value = line.split("\t")
    if key == "!TOTAL":
        total += int(value)
    elif key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            if cur_count >= support_count:
                print '%s\t%s\t%s' % (cur_key,cur_count,str(cur_count/total))
        cur_key = key
        cur_count = int(value)

if cur_count >= support_count:
    print '%s\t%s\t%s' % (cur_key,cur_count,str(cur_count/total))
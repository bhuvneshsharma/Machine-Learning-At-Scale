#!/usr/bin/env python
from operator import itemgetter
import sys
cur_key = None
cur_count = 0
sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")

current_word = None
current_count = 0
word = None
max_len = 0
# input comes from STDIN
for line in sys.stdin:
    key, value = line.split("\t")
    if key == "!MAX_LENGTH":
        if int(value) > max_len:
            max_len = int(value)
    elif key == cur_key:
        cur_count += int(value)
    else:
        if cur_key:
            print '%s\t%s' % (cur_key, cur_count)
        cur_key = key
        cur_count = int(value)

print '%s\t%s' % (cur_key, cur_count)

print "!MAX_LENGTH\t%s" % (max_len)  
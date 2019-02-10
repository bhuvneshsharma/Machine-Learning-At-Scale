#!/usr/bin/env python
import sys
import re
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

max_len = 0
for line in sys.stdin:
    line = line.strip()
    words = line.split()
    if len(words) > max_len:
        max_len = len(words)
    for word in words:
        sys.stderr.write("reporter:counter:Mapper Counters,Total,1\n")
        print '%s\t%d' % (word, 1)
print "!MAX_LENGTH\t%s" % (max_len)  
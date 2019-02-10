#!/usr/bin/env python

from __future__ import division
import sys

total = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    if key == '!MAX_LENGTH':
        continue
    else:
        total += int(value)
        print '%s\t%s' % (key, value)

print '!TOTAL\t', total
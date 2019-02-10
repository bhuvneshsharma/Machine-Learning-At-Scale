#!/usr/bin/env python

from __future__ import division
import sys

total = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split('\t')
    if key == '!TOTAL':
        total += int(value)
    else:
        print '%s\t%s\t%s' % (key, value, int(value)/total)
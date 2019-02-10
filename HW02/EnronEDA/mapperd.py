#!/usr/bin/env python
"""
Mapper reverses order of key(word) to class 
INPUT:
    word \t isSpam \t count
OUTPUT:
    isSpam \t count \t word   
"""
import re
import sys

# read from standard input
for line in sys.stdin:
    line = line.strip()
    word, isSpam, count  = line.split('\t')
    print '%s\t%s\t%s' % (isSpam, count, word)

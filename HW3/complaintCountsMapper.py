#!/usr/bin/env python

# START STUDENT CODE HW31MAPPER

"""
Reads stdin line by line and prints for each prodcut, product \t 1
INPUT:
    text file
OUTPUT:
    product \t 1
USAGE:
    python complaintCountsMapper.py < textfile
"""

import sys
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

# read from standard input
for line in sys.stdin:
    token = line.strip().split(',')
    if token[1].lower() == 'debt collection':
        print '%s\t%s' % (token[1].lower(), 1)
        sys.stderr.write("reporter:counter:Product,debt collecction,1\n")
    elif token[1].lower() == 'mortgage':
        print '%s\t%s' % (token[1], 1)
        sys.stderr.write("reporter:counter:Product,mortgage,1\n")
    else:
        print '%s\t%s' % ('others', 1)
        sys.stderr.write("reporter:counter:Product,others,1\n")
        
# END STUDENT CODE HW31MAPPER
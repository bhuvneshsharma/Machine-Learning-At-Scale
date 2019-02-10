#!/usr/bin/env python
# START STUDENT CODE HW32AMAPPER

import sys
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

# read from standard input
for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        print '%s\t%s' % (word, 1)
        #sys.stderr.write("reporter:counter:Product,debt collecction,1\n")


# END STUDENT CODE HW32AMAPPER
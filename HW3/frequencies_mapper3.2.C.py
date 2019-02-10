#!/usr/bin/env python
# START STUDENT CODE HW32CFREQMAPPER

import re
import sys
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

total = 0 

# read from standard input
for line in sys.stdin:
    key, value = line.strip().split()
    total += int(value)
    print '%s\t%s' % (key, value)

print '%s\t%s' % ('!total', total)

# END STUDENT CODE HW32CFREQMAPPER
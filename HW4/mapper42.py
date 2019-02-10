#!/usr/bin/env python

#START STUDENT CODE42

import sys
#sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

for line in sys.stdin:
    token = line.strip().split(',')
    if token[0] == 'C':
        id = token[2]
    elif token[0] == 'V':
        print '%s,%s,%s,%s,%s' % (token[0], token[1], token[2],'C',id)
    else:
        continue
        

#END STUDENT CODE42
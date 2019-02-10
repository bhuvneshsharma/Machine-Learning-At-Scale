#!/usr/bin/env python
# START STUDENT CODE HW32CMAPPER

import re
import sys
sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")

# read from standard input
for line in sys.stdin:
    issue = line.strip().split(',')[3]
    words = re.findall(r'[a-z]+', issue.lower())
    for word in words:
        print '%s\t%s' % (word, 1)
        sys.stderr.write("reporter:counter:Issue,word,1\n")

# END STUDENT CODE HW32CMAPPER
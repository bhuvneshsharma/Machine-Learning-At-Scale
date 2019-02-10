
import sys
import re

total = 0 

for line in sys.stdin:
    word, count = line.split()
    if word == '"!TOTAL"':
        total += int(count)
    else:
        print word, count, total
 
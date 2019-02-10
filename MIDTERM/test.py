
import sys
import re

for line in sys.stdin:
    index = line.split('.',1)[0]
    print index
    letter_list = re.sub(r"[^A-Za-z]+", '', line).lower()
    print letter_list
    
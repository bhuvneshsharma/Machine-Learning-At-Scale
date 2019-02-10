
import sys, ast

for line in sys.stdin:
    values = line.strip().split()
    print "\nBasic Statistics on Data"
    print "-"*80
    print "Number of nodes: ", values[0]
    print "Number of links: ", ast.literal_eval(values[1])[0]
    print "Average links per node: ", ast.literal_eval(values[1])[1]
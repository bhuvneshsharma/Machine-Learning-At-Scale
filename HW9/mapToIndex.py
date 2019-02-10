#!/opt/anaconda2/bin python

from itertools import izip
import sys

SOURCE = sys.argv[1]
LOOKUP = sys.argv[2]
ITER = sys.argv[3]

lookup = {}

for line in open(LOOKUP).read().strip().split('\n'):
    line = line.split('\t')
    key = line[1]
    value = line[0]
    lookup[key] = value

print '\n'
print 'Top 100 pages with {} iterations'.format(ITER)
print '='*80

print '{0: <10} | {1: <10} | {2}'.format('pagerank', 'id', 'article')
    
for line in open(SOURCE).read().strip().split('\n'):
    line = line.split('\t')
    node = line[1].replace('\"', "")
    pr = line[0]
    article = lookup.get(node, "")
    
    print '{0: <10} | {1: <10} | {2}'.format(round(float(pr), 5), node, article)

print '='*80
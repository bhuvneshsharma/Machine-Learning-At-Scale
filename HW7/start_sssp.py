#!/opt/anaconda/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, ast

class MRstart_sssp(MRJob):

    # Set initial node conditions
    def configure_options(self):
        super(MRstart_sssp, self).configure_options()
        self.add_passthrough_option('--startNode', default='1')

    # Process lines
    def mapper(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = ast.literal_eval(fields[1])
        if name == self.options.startNode:
            yield name, (neighbors, 0, [name], 'Q')
        else:
            yield name, (neighbors, sys.maxint, [], 'U')

if __name__ == '__main__':
    MRstart_sssp.run()
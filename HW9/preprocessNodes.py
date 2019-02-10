#!/opt/anaconda/bin python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class MRpreprocess(MRJob):
    
    OUTPUT_PROTOCOL = JSONProtocol
    
    def configure_options(self):
        super(MRpreprocess, self).configure_options()
        self.add_passthrough_option('--N', default=None, type='float', help='total number of nodes')

    def mapper(self, _, line):
        node, out_links = line.split('\t')
        pr = 1 / self.options.N
        yield node, out_links + "|" + str(pr)


if __name__ == '__main__':
    MRpreprocess.run()
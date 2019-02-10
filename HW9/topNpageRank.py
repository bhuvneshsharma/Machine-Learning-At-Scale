#!/opt/anaconda/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast,sys

class MRtopNpageRank(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init = self.init,
                mapper = self.m_rank,
                mapper_final = self.final,
                reducer_init = self.init,
                reducer = self.r_rank,
                reducer_final = self.final,
                jobconf={
                    'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                    'mapreduce.partition.keycomparator.options': '-k1,1rn',
                    'mapreduce.job.maps': '100',
                    'mapreduce.job.reduces': '1'
                }
            )]
    
    # set command line option to accept start and end node
    def configure_args(self):
        super(MRtopNpageRank, self).configure_args()
        self.add_passthru_arg('--N', default=100, type=int, help='top N')
    
    def init(self):
        self.N = self.options.N
        self.top_N = []
    
    def m_rank(self, _, line):
        node, pr_out_links = line.split("\t")
        node = node.replace("\"", "")
        out_links, pr = map(ast.literal_eval, pr_out_links.replace("\"","").split('|'))
        
        self.top_N.append((pr, node))
        if len(self.top_N) > self.N:
            self.top_N.sort(key=lambda x: -float(x[0]))
            self.top_N = self.top_N[:self.N]
            
    def r_rank(self, pr, nodes):
        for node in nodes:
            self.top_N.append((float(pr), node))
        if len(self.top_N) > self.N:
            self.top_N.sort(key=lambda x: -float(x[0]))
            self.top_N = self.top_N[:self.N]
            
    def final(self):
        for pr, node in self.top_N:
            yield pr, node
    
if __name__ == '__main__':
    MRtopNpageRank.run()
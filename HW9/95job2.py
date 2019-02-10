#!/opt/anaconda2/bin python

from mrjob.job import MRJob
from mrjob.step import MRStep

class MR95job2(MRJob):     
    
    def steps(self):
        return ([MRStep(jobconf={
                    'mapreduce.job.maps': '100',
               },
                        mapper_init = self.mapper_init,
                        mapper = self.mapper),
                ])    
    
    def mapper_init(self):
        self.lookup = { k:v for k, v in (line.split("\t") for line in 
                open('/home/lteo01/SP18-1-maynard242/HW9/wikipedia/index_1.txt').read().strip().split('\n')) }
    
    def mapper(self, _, line):
        node, out_links = line.split("\t")
        yield node, out_links + "|" + str(self.lookup.get(node, 0))
    
if __name__ == '__main__':
    MR95job2.run()
#!/opt/anaconda2/bin python

from mrjob.job import MRJob
from mrjob.step import MRStep

# Process index file to reduce size

class MR95job1(MRJob):      
    
    def steps(self):
        return ([MRStep(jobconf={
                    'mapreduce.job.maps': '100',
               },
                        mapper = self.mapper),
                ])
                
    
    def mapper(self, _, line):
        line = line.split('\t')
        key = line[1]
        value = len(line[0])%10
        yield key, value
    
if __name__ == '__main__':
    MR95job1.run()
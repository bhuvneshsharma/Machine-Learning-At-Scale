
import sys
import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol

WORD_RE = re.compile(r"[\w']+")
  
class MR32C1(MRJob):

    SORT_VALUES = True
    #INTERNAL_PROTOCOL = RawProtocol
    #OUTPUT_PROTOCOL = RawProtocol

    def steps(self):
        
        JOBCONF_STEP1 = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'stream.num.map.output.key.fields':2,
                'stream.map.output.field.separator':'\t',
                'mapreduce.partition.keycomparator.options': '-k1,1',
                }  
        
        JOBCONF_STEP2 = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'stream.num.map.output.key.fields':3,
                'stream.map.output.field.separator':'\t',
                'mapreduce.partition.keycomparator.options': '-k1,1',
                'mapreduce.job.reduces':2,
                'partitioner':'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'    
                }  
        
        
        
        return [
        MRStep(jobconf=JOBCONF_STEP1,
               mapper=self.mapper,
               combiner=self.combiner,
               reducer=self.reducer,
              ),
        MRStep(jobconf=JOBCONF_STEP2, 
              mapper=self.mapper2,
              #reducer_init=self.reducer2_init,
              #reducer=self.reducer2,
              ),
            
            
            
        ]


    def mapper(self, _, line):
        issue = line.strip().split(',')[3]
        words = WORD_RE.findall(issue.lower())
        for word in words:
            yield '!Total', 1 
            yield word, 1
            
    def combiner(self, word, counts):
        yield word, sum(counts)
        
    def reducer(self, word, counts):
        yield word, sum(counts)
        
    def mapper2(self, word, counts):
        if word == '!Total':
            yield 'a', '!Total'+'\t'+str(counts)
            yield 'b', '!Total'+'\t'+str(counts)
        
        if int(counts) > 2500:
            yield 'a', word+'\t'+str(counts)
        else:
            yield 'b', word+'\t'+str(counts)
   
    def reducer2_init(self):
        self.total = 0

    def reducer2(self, key, value):
        for v in value:
            k, val = v.split()
            if k == '!Total':
                self.total = int(val)
            else:
                yield k, (key, int(val), self.total)
                
                    
if __name__ == '__main__':
    MR32C1.run()

import sys
import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

WORD_RE = re.compile(r"[\w']+")
  
class MR32C(MRJob):

    SORT_VALUES = True
    #OUTPUT_PROTOCOL = RawProtocol

    def steps(self):
        
        JOBCONF_STEP1 = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'stream.num.map.output.key.fields':'2',
                'stream.map.output.field.separator':'\t',
                'mapreduce.partition.keycomparator.options': '-k1',
                'mapreduce.job.reduces': '1', 
                }  
        
        JOBCONF_STEP2 = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'stream.num.map.output.key.fields':'2',
                'stream.map.output.field.separator':'\t',
                'mapreduce.partition.keycomparator.options': '-k1nr',
                'mapreduce.job.reduces': '1', 
                }  
        
        return [
        MRStep(jobconf=JOBCONF_STEP1, 
               mapper=self.mapper,
               combiner=self.combiner,
               reducer_init=self.reducer_init,
               reducer=self.reducer,
              ),
        MRStep(jobconf=JOBCONF_STEP2, 
              mapper=self.mapper2,)     
        ]


    def mapper(self, _, line):
        issue = line.strip().split(',')[3]
        words = WORD_RE.findall(issue.lower())
        for word in words:
            yield '!TOTAL', 1
            yield word, 1
            
    def combiner(self, word, counts):
        yield word, sum(counts)
   
    def reducer_init(self):
        self.total = 0

    def reducer(self, word, counts):
        if word == '!TOTAL':
            self.total = sum(counts)
        partial = sum(counts)
        yield word, (partial, float(partial)/self.total)
        
    def mapper2(self, word, counts):
        if word != '!TOTAL':
            yield (counts[0], (counts[1], word))
           
                    
if __name__ == '__main__':
    MR32C.run()

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
 
WORD_RE = re.compile(r"[\w']+")
 
class MRWordFreqCount(MRJob):
    SORT_VALUES = True
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            self.increment_counter('group', 'mapper', 1)
            yield word.lower(), 1
            
    def jobconfqqqq(self):  #assume we had second job to sort the word counts in decreasing order of counts
        orig_jobconf = super(MRWordFreqCount, self).jobconf()        
        custom_jobconf = {  #key value pairs
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k2,2nr',
            'mapred.reduce.tasks': '1',
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf


    def combiner(self, word, counts):
        self.increment_counter('group', 'combiner', 1)
        yield word, sum(counts)

    def reducer(self, word, counts):
        self.increment_counter('group', 'reducer', 1)
        yield word, sum(counts)

    def steps(self):
        return [MRStep(
                mapper = self.mapper, 
                combiner = self.combiner,
                reducer = self.reducer,
                #,
#                jobconf = self.jobconfqqqq
 
#            jobconf = {'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
#                       'mapred.text.key.comparator.options':'-k1r',
#                       'mapred.reduce.tasks' : 1}   
       
        
            )]
     


if __name__ == '__main__':
    MRWordFreqCount.run()
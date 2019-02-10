
import sys
import re
from mrjob.job import MRJob
  
class MR32B(MRJob):
    
    SORT_VALUES = True

    def mapper(self, _, line):
        issue = line.strip().split(',')[3]
        words = re.findall(r'[a-z]+', issue.lower())
        for word in words:
            sys.stderr.write("reporter:counter:Issue,word,1\n")
            sys.stderr.write("reporter:counter:Mapper,Calls,1\n")
            yield word, 1
        
    def reducer(self, key, counts):
        sys.stderr.write("reporter:counter:Reducer,Calls,1\n")
        yield key, sum(counts)

if __name__ == '__main__':
    MR32B.run()
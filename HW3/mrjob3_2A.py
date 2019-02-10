
import sys
from mrjob.job import MRJob
  
class MR32A(MRJob):
    
    SORT_VALUES = True

    def mapper(self, _, line):
        words = line.strip().split()
        for word in words:
            sys.stderr.write("reporter:counter:Mapper,Calls,1\n")
            yield word, 1
        
    def reducer(self, key, counts):
        sys.stderr.write("reporter:counter:Reducer,Calls,1\n")
        yield key, sum(counts)

if __name__ == '__main__':
    MR32A.run()
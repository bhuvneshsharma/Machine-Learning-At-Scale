
import sys
from mrjob.job import MRJob
  
class MR31(MRJob):

    def mapper(self, _, line):
        token = line.strip().split(',')
        if token[1].lower() == 'debt collection':
            yield token[1].lower(), 1
            sys.stderr.write("reporter:counter:Product,debt collecction,1\n")
        elif token[1].lower() == 'mortgage':
            yield token[1], 1
            sys.stderr.write("reporter:counter:Product,mortgage,1\n")
        else:
            yield 'others', 1
            sys.stderr.write("reporter:counter:Product,others,1\n")
     
    def combiner(self, key, counts):
        yield key, sum(counts)

    #hello, (1,1,1,1,1,1): using a combiner? NO and YEs
    def reducer(self, key, counts):
        yield key, sum(counts)

if __name__ == '__main__':
    MR31.run()
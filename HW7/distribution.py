
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

# Distribution
class MRdistribution(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer,
            )
        ] 
    
    def mapper(self, _, line):
        node, info = line.strip().split('\t')
        edges = ast.literal_eval(info)
        count = len(edges.items())
        yield int(count), 1
        
    def reducer(self, count, values):
        yield int(count), sum(values)

if __name__ == '__main__':
    MRdistribution.run()
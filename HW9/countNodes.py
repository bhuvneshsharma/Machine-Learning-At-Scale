#!/opt/anaconda2/bin python

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRcountNodes(MRJob):
    
    def steps(self):
        return (
                [MRStep(
                    mapper = self.mapper,
                    combiner = self.combiner,
                    reducer = self.reducer),
                 MRStep(reducer = self.reducer_2)
                ]
        )
    
    def mapper(self, _, line):
        v, edges = line.strip().split('\t')
        edges = eval(edges)
        for edge in edges:
            yield edge, 1
        yield v, 1

    def combiner(self, edge ,count):
        yield edge, max(count)
            
    def reducer(self, edge ,count):
        yield None, max(count)

    def reducer_2(self, edge ,count):
        yield 'num_of_nodes', sum(count)
        
if __name__ == '__main__':
    MRcountNodes.run()
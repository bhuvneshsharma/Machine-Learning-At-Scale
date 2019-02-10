
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

# Count number of nodes and links
class MRcounts(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer,
            )
        ] 
    
    # Number of links for node
    def mapper(self, _, line):
        node, info = line.strip().split('\t')
        edges = ast.literal_eval(info)
        count = len(edges.items())
        yield None, count
        
    # Sum links
    def reducer(self, _, counts):
        cnts = list(counts)
        yield len(cnts), (sum(cnts), float(sum(cnts))/len(cnts))

if __name__ == '__main__':
    MRcounts.run()
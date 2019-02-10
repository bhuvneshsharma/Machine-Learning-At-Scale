#!/opt/anaconda/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, ast

class MRsssp(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_1,
            ),
            MRStep(mapper=self.mapper_2,
                   reducer=self.reducer_2,
            )
    ] 
    

    # Set initial node conditions
    def configure_options(self):
        super(MRsssp, self).configure_options()
        self.add_passthrough_option('--startNode', default='1')

    # Process lines
    def mapper_1(self, _, line):
        fields = line.strip().split('\t')
        name = fields[0]
        neighbors = ast.literal_eval(fields[1])
        if name == self.options.startNode:
            yield name, (neighbors, 0, [name], 'Q')
        else:
            yield name, (neighbors, sys.maxint, [], 'U')
            
    # Process lines
    def mapper_2(self, key, values):

        # Split text to get our data  
        name = key
        #values = ast.literal_eval(values)
        neighbors = values[0]
        distance = int(values[1])
        path = values[2]
        status = values[3]
        
        # If this node is queued, contine expand
        if status == 'Q':
            yield name, [neighbors, distance, path, 'V']
            if neighbors:
                for node in neighbors:
                    temp_path = list(path)
                    temp_path.append(node)
                    yield node, [None, distance + 1, temp_path, 'Q']
        else:
            yield name, [neighbors, distance, path, status]

    # Cycle through nodes
    def reducer_2(self, key, values):
        neighbors = {}
        distance = sys.maxint
        status = None
        path = []

        for val in values:
            if val[3] == 'V':
                neighbors = val[0]
                distance = val[1]
                path = val[2]
                status = val[3]
                break
            elif val[0]:
                neighbors = val[0]
                if status != 'Q':
                    status = val[3]
            else:
                path = val[2]
                status = val[3]
                
            # Update minimum distance if necessary
            distance = min(distance, val[1])

        yield key, [neighbors, distance, path, status]

if __name__ == '__main__':
    MRsssp.run()
#!/opt/anaconda/bin/python

from mrjob.job import MRJob
from mrjob.step import MRStep
import sys, ast

class MRsssp(MRJob):
    
    # Process lines
    def mapper(self, _, line):

        # Split text to get our data
        fields = line.strip().split('\t')     
        name = str(ast.literal_eval(fields[0]))
        value = ast.literal_eval(fields[1])
        neighbors = value[0]
        distance = int(value[1])
        path = value[2]
        status = value[3]
        
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
    def reducer(self, key, values):
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
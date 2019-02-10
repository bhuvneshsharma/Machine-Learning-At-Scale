#!/usr/bin/env python
#START STUDENT CODE44

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re    
from operator import itemgetter
 
class mostFrequentVisitors(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol # Not strictly necessary, but the output is prettier this way
    
    def steps(self):
      return [MRStep(
              mapper = self.mapper,
              combiner = self.combiner,
              reducer_init = self.reducer_init,
              reducer = self.reducer_count
              ),
             MRStep(
              reducer = self.reducer_max
            )]
    
    def mapper(self, _, line):
      data = re.split(",",line)
      pageID = data[1]
      custID = data[4]
      yield (custID,pageID), 1
        
    def combiner(self,key,counts):
      custID = key[0]
      pageID = key[1]
      visitCount = sum(counts)
      yield (custID,pageID), visitCount
    
    # An in-memory reducer join - next week we'll implement a variety of joins and explore trade-offs.
    def reducer_init(self):
      self.URLs = {}
      with open("anonymous-msweb-URLS.data", "r") as IF:
          for line in IF:
              line = line.strip()
              data = re.split(",",line)
              URL = data[4]
              pageID = data[1]
              self.URLs[pageID] = URL

    def reducer_count(self,key,counts):
      custID = key[0]
      pageID = key[1]
      visitCount = sum(counts)
      yield (pageID,self.URLs[pageID]),(custID,visitCount)
      
      
    def reducer_max(self,key,values):
      maxVisits = max(values,key=itemgetter(1))
      yield None, str(key[1])+","+(key[0])+","+str(maxVisits[0])+","+str(maxVisits[1])
        
        
if __name__ == '__main__':
    mostFrequentVisitors.run()

#END STUDENT CODE44
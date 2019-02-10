#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
import sys
import re
import mrjob
import itertools
import json
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRbuildStripes(MRJob):

  #START SUDENT CODE531_STRIPES

    # Init Settings
    SORT_VALUES = True
    OUTPUT_PROTOCOL = RawProtocol
    
    def steps(self):
        return [
        MRStep(mapper=self.mapper,
               reducer=self.reducer
              ),
        ]
    
    
    def mapper(self, _, line):
        sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
        H = {}
        words = re.findall(r'[a-z\']+', line.lower())
        counts = re.findall(r'[0-9]+', line)[0]

        for subset in itertools.combinations(sorted(set(words)), 2):
        
            if subset[0] not in H.keys():
                H[subset[0]] = {}
                H[subset[0]][subset[1]] = counts 
            elif subset[1] not in H[subset[0]]:
                H[subset[0]][subset[1]] = counts
            else:
                H[subset[0]][subset[1]] = counts

            if subset[1] not in H.keys():
                H[subset[1]] = {}
                H[subset[1]][subset[0]] = counts 
            elif subset[0] not in H[subset[1]]:
                H[subset[1]][subset[0]] = counts
            else:
                H[subset[1]][subset[0]] = counts
        
        for key in H.keys():
            yield key, H[key]
         
            
    def reducer(self, key, values):
        sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
        H = {}
        for value in values:
            for k, v in value.iteritems():
                if k in H.keys():
                    H[k] += v
                else:
                    H[k] = v
        
        yield key, json.dumps(H)
        
  #END SUDENT CODE531_STRIPES
    
if __name__ == '__main__':
    MRbuildStripes.run()
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
    H = {}
    
    def steps(self):
        
        JOBCONF_STEP = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'mapreduce.partition.keycomparator.options': '-k1'
                }  
        return [
        MRStep(jobconf=JOBCONF_STEP, 
               mapper=self.mapper,
               reducer=self.reducer),
        MRStep(mapper=self.mapper_build
              )
        ]
    
    
    def mapper(self, _, line):
        sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
        
        words = re.findall(r'[a-z\']+', line.lower())
        counts = re.findall(r'[0-9]+', line)[0]

        for subset in itertools.combinations(sorted(set(words)), 2):
        
            if subset[0] not in self.H.keys():
                self.H[subset[0]] = {}
                self.H[subset[0]][subset[1]] = counts 
            elif subset[1] not in self.H[subset[0]]:
                self.H[subset[0]][subset[1]] = counts
            else:
                self.H[subset[0]][subset[1]] = counts

            if subset[1] not in self.H.keys():
                self.H[subset[1]] = {}
                self.H[subset[1]][subset[0]] = counts 
            elif subset[0] not in self.H[subset[1]]:
                self.H[subset[1]][subset[0]] = counts
            else:
                self.H[subset[1]][subset[0]] = counts
        
        for key in self.H:
            yield key, self.H[key]
         
            
    def reducer(self, key, values):
        sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
        yield key, values
        
    
    def mapper_build(self, key, values):    
        sys.stderr.write("reporter:counter:Final Mapper Counters,Calls,1\n")
        z = {}
        for i in values:
            z.update(i)
        z = {k:int(v) for k, v in z.items()}
        yield key, z
  
  #END SUDENT CODE531_STRIPES
    
if __name__ == '__main__':
    MRbuildStripes.run()
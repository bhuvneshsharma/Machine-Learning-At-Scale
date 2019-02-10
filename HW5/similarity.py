#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
import sys
import collections
import re
import json
import math
import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRsimilarity(MRJob):
  
    #START SUDENT CODE531_SIMILARITY
    
    SORT_VALUES = True
    
    def steps(self):
        
        JOBCONF_STEP = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'mapreduce.partition.keycomparator.options': '-k1,1nr',
                }  
        return [
        MRStep(#jobconf=JOBCONF_STEP, 
               mapper=self.mapper_pair_sim,
               reducer=self.reducer_pair_sim
            ),
        MRStep(jobconf=JOBCONF_STEP, 
               mapper=None,
               reducer=self.reducer_pair_sim2
            ), 
        MRStep(jobconf=JOBCONF_STEP, 
               mapper=self.mapper_pair_sim2,
               reducer=self.reducer_pair_sim_final
              )
        ]

    def mapper_pair_sim(self, _, line):     
        sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
        line = line.strip()
        index, posting = line.split('\t')
        posting = json.loads(posting)
        posting = dict(posting)
        
        for docs in itertools.combinations(sorted(posting.keys()), 2):
            yield (docs, posting[docs[0]], posting[docs[1]]), 1
        

    def reducer_pair_sim(self, key, values):
        sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
        total = sum(values)
        cosine = total/(np.sqrt(key[1])*np.sqrt(key[2]))
        jacard = total/(key[1]+key[2]-total)
        overlap = total/min(key[1],key[2])
        dice = 2*total/(key[1]+key[2])
        
        yield (cosine,jacard,overlap,dice), (key[0][0]+' - '+key[0][1]) 


    def reducer_pair_sim2(self, key, values):
        sys.stderr.write("reporter:counter:Final Reducer Counters,Calls,1\n")
        for value in values:
            avg = np.mean(key)
            yield avg, (value, key[0], key[1], key[2], key[3])
    
    def mapper_pair_sim2(self,key,values):
        sys.stderr.write("reporter:counter:Final Mapper Counters,Calls,1\n")
        yield key, values
        
    def reducer_pair_sim_final(self,key,values):
        sys.stderr.write("reporter:counter:Final Reducer Counters,Calls,1\n")
        yield key, values
        
        
    #END SUDENT CODE531_SIMILARITY

if __name__ == '__main__':
    MRsimilarity.run()
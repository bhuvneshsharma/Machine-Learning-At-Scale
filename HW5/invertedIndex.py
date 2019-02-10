#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
import collections
import sys
import re
import json
import math
import numpy as np
import itertools
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRinvertedIndex(MRJob):
    
    #START SUDENT CODE531_INV_INDEX
    
    SORT_VALUES = True
    
    def steps(self):
        
        JOBCONF_STEP = {
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'mapreduce.partition.keycomparator.options': '-k1'
                }  
        return [
        MRStep(jobconf=JOBCONF_STEP, 
               mapper=self.mapper,
               reducer=self.reducer)
        ]
    
    def mapper(self, _, line):
        sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
        
        tokens = line.strip().split('\t')
        value_dict = json.loads(tokens[1])
        term_len = len(value_dict)
        
        for key in value_dict.keys():
            yield key, [tokens[0], term_len]

            
    def reducer(self, key, values):
        sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
        out = []
        for value_dict in values:
            value_dict[0] = value_dict[0].replace('"','')
            out.append(value_dict)
        
        yield key, out


  #END SUDENT CODE531_INV_INDEX
        
if __name__ == '__main__':
    MRinvertedIndex.run() 
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
from operator import itemgetter
from mrjob.protocol import RawProtocol
from mrjob.protocol import JSONProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import logging

class MRsimilarity(MRJob):
    
    SORT_VALUES = True
    INTERNAL_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol
    
    def __init__(self, *args, **kwargs):
        super(MRsimilarity, self).__init__(*args, **kwargs)
        self.N = 25
        self.NUM_REDUCERS = 2
        
    def steps(self):
        JOBCONF_STEP_1 = {
                "mapreduce.job.reduces": "2",
                "mapreduce.job.maps": "2",
                }
        
        JOBCONF_STEP_2 = {
                'stream.num.map.output.key.fields':3,
                'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                'stream.map.output.field.separator':"\t",
                'mapreduce.partition.keypartitioner.options':'-k1,1',
                'mapreduce.partition.keycomparator.options':'-k2,2nr -k3,3',
                'mapred.reduce.tasks': self.NUM_REDUCERS,
                'partitioner':'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
                "mapreduce.job.reduces": str(self.NUM_REDUCERS),
                "SORT_VALUES":True,
                "INTERNAL_PROTOCOL":"RawProtocol",
                "OUTPUT_PROTOCOL":"RawProtocol"
                }  
         
        return [
        MRStep(jobconf=JOBCONF_STEP_1,
               mapper=self.mapper_pair_sim,
               combiner = self.combiner_pair_sim,
               reducer=self.reducer_pair_sim
            ),
        MRStep(jobconf=JOBCONF_STEP_2, 
               mapper_init=self.mapper_sort_init,
               mapper= self.mapper_sort,
               reducer=self.reducer_sort
            )
        ]

    def mapper_pair_sim(self, _, line):     
        sys.stderr.write("reporter:counter:Mapper Counters,Calls,1\n")
        line = line.strip()
        index, posting = line.split('\t')
        posting = json.loads(posting)
        posting = dict(posting)
        logging.warning(line)
        logging.warning(posting)
        for docs in itertools.combinations(sorted(posting.keys()), 2):
            yield ",".join([docs[0],docs[1], str(posting[docs[0]]), str(posting[docs[1]])]),str(1)
        
    
    def combiner_pair_sim(self, key, values):
        yield key,str(sum([int(v) for v in values]))
        
    def reducer_pair_sim(self, key, values):
        sys.stderr.write("reporter:counter:Reducer Counters,Calls,1\n")
        total = sum([float(v) for v in values])
        key = key.split(",")
        key[2] = float(key[2])
        key[3] = float(key[3])
        cosine = total/(np.sqrt(key[2])*np.sqrt(key[3]))
        jacard = total/(key[2]+key[3]-total)
        overlap = total/min(key[2],key[3])
        dice = 2*total/(key[2]+key[3])
        yield None, str(np.mean([cosine, jacard, overlap, dice]))+"\t"+"[" +",".join(["\""+ key[0]+" - "+key[1]+"\"",str(cosine),str(jacard),str(overlap),str(dice)])+"]"

    def mapper_sort_init(self):
        def makeKeyHash(key, num_reducers):
            byteof = lambda char: int(format(ord(char), 'b'), 2)
            current_hash = 0
            for c in key:
                current_hash = (current_hash * 31 + byteof(c))
            return current_hash % num_reducers
        
        # printable ascii characters, starting with 'A'
        keys = [str(unichr(i)) for i in range(65,65+self.NUM_REDUCERS)]
        partitions = []

        for key in keys:
            partitions.append([key, makeKeyHash(key, self.NUM_REDUCERS)])

        parts = sorted(partitions,key=itemgetter(1))
        self.partition_keys = list(np.array(parts)[:,0])

        self.partition_file = np.arange(0,self.N,self.N/(self.NUM_REDUCERS))[::-1]

        
    
    def mapper_sort(self, key, value):
        keyFloatScaled = np.floor(float(key)*self.N)
        # Prepend the approriate key by finding the bucket, and using the index to fetch the key.
        for idx in xrange(self.NUM_REDUCERS):
            if keyFloatScaled > self.partition_file[idx]:
                yield str(self.partition_keys[idx]),key+" \t "+value
                break
        
    def reducer_sort(self, key, values):
        sys.stderr.write("reporter:counter:Intermediate Reducer Counters,Calls,1\n")
        for value in values:
            yield None, value

if __name__ == '__main__':
    MRsimilarity.run()
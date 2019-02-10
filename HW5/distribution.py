#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class distribution(MRJob):
    
    #### TODO: divide the counts by 1000s to make the graph more readable
    #### TODO: split the lengths into buckets <10, <25, <50, <75, <100
    def mapper(self, _, line):
        fields = line.strip("\n").split("\t")
        yield len(fields[0]), int(fields[1])
    
    def combiner(self,length, counts):
        yield length, sum(counts)
        
    def reducer(self,length, counts):
        yield length, sum(counts)
    
    def reducer_sort(self, key, values):
        yield key, list(values)[0]
        
    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                combiner = self.combiner,
                reducer = self.reducer,
                jobconf = {
                    "mapreduce.job.reduces": "4",
                }
            ),
            MRStep(
                reducer = self.reducer_sort,
                jobconf = {
                    "SORT_VALUES":True,
                    "mapreduce.job.reduces": "1",
                    "stream.num.map.output.key.fields": 1,
                    "mapreduce.job.output.key.comparator.class" : "org.apache.hadoop.mapred.lib.KeyFieldBasedComparator",
                    "mapreduce.partition.keycomparator.options":"-k1,1nr",
                }
            )
        ]
    
if __name__ == '__main__':
    distribution.run()
#!~/opt/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import re
import mrjob
import json
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import itertools

class MRbuildStripes(MRJob):
    
    SORT_VALUES = True
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer,
                jobconf = {
                    "mapreduce.job.reduces": "64",
                    "mapreduce.job.maps": "64",
#                     "SORT_VALUES":True
                }
            ),
            MRStep(
                reducer=self.reducer_2,
                jobconf = {
                    "mapreduce.job.reduces": "1",
                    "SORT_VALUES":True
                }
            )
            
        ]
    
    def mapper_init(self):
        self.idx = 9001  # To define when feature set starts
        self.filename = 'ten_thousand_20.json'
        
        self.top_words = []
        self.features = []
        #with open('features_20.json', 'r') as infile:
        #    self.features = json.loads(infile.read())
        with open(self.filename, 'r') as infile:
            self.top_words = json.loads(infile.read())
            self.features =self.top_words[self.idx:]

    def mapper(self, _, line):
        fields = line.lower().strip("\n").split("\t")
        words = fields[0].split(" ")
        occurrence_count = int(fields[1])
        filtered_words = [word.decode('utf-8', 'ignore') for word in words if word.decode('utf-8', 'ignore') in self.top_words]
        for subset in itertools.combinations(sorted(set(filtered_words)), 2):
            if subset[0] in self.top_words and subset[1] in self.features:
                yield subset[0], (subset[1], occurrence_count)
            if subset[1] in self.top_words and subset[0] in self.features:
                yield subset[1], (subset[0], occurrence_count)
    
    def reducer(self, word, occurrence_counts):
        stripe = {}
        for other_word, occurrence_count in occurrence_counts:
            stripe[other_word] = stripe.get(other_word,0)+occurrence_count
        yield word, stripe
    
    def reducer_2(self, key, values):
        yield str(key), list(values)[0]
        
if __name__ == '__main__':
    MRbuildStripes.run()
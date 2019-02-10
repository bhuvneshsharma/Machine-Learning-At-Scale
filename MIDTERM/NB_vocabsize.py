#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import sys
import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep

class Naivebayes_count(MRJob):
    
    def mapper(self, _, line):
        
        words = re.findall(r'[a-z0-9]+', line.lower())[2:]
        for word in words:
            yield None, word
            
    def reducer_init(self):
        self.l = []
            
    def reducer(self, key, value):
        for v in value:
            self.l.append(v)
        yield None, len(set(self.l))
        
    def steps(self):
        return [MRStep(
                       mapper=self.mapper,
                       reducer_init=self.reducer_init,
                       reducer=self.reducer,
                      ),
               ]
        
if __name__ == '__main__':
    Naivebayes_count.run()
    
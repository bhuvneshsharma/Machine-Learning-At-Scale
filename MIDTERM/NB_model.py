#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
import json
import re
import sys
import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep  


class Naivebayes(MRJob):
    
    SORT_VALUES = True
    

    def mapper1_init(self):
        self.docTotals = np.array([0,0])
        self.wordTotals = np.array([0,0])

    def mapper1(self, _, line):
        
        tokens = re.findall(r'[a-z0-9]+', line.lower())
        class_ = tokens[0]
        docid = tokens[1]
        words = tokens[2:]
        
        class_ = 1 if class_ == 'spam' else 0
        increment = [1,0] if class_ == 0 else [0,1]
         
        self.docTotals = increment
        self.wordTotals = np.array(increment)*len(words)
        
        for word in words:
            yield word, increment
            
        yield '!docTotals', self.docTotals
        yield '!wordTotals', self.wordTotals

    def reducer1_init(self):
        k = 1
        V = 3
        self.k = np.array([k,k])
        self.total = np.array([V,V])
        self.docTotals = 0
        self.wordTotals = 0
        
    
    def reducer1(self, key, value):
        curr_sum = 0
        for v in value:
            curr_sum += np.array(v)
            
        if key == '!docTotals':
            yield key, (curr_sum, curr_sum/sum(curr_sum))
            
        elif key == '!wordTotals':
            self.wordTotals = curr_sum
    
        else:
            yield key, (curr_sum, (curr_sum+self.k)/(self.wordTotals+self.total))

        

    def steps(self):
        return [MRStep(mapper_init=self.mapper1_init,
                       mapper=self.mapper1,
                       reducer_init=self.reducer1_init,
                       reducer=self.reducer1,
                      ),
               
               ]

if __name__ == '__main__':
    Naivebayes.run()
    
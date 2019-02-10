#!~/anaconda2/bin/python
# -*- coding: utf-8 -*-

from __future__ import division
import re
import numpy as np
from collections import defaultdict
import mrjob
from mrjob.protocol import RawProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep

class mostLeastDenseWords(MRJob):

    # START STUDENT CODE 5.6.1.C
    
    def mapper_init(self):
        self.word_dense = defaultdict(list)
        
    def mapper(self, _, line):
        [ngram, count, page, book] = line.lower().strip().split('\t')
        for word in ngram.split():
            if word in self.word_dense:
                self.word_dense[word][0] += int(count)
                self.word_dense[word][1] += int(page)
            else:
                self.word_dense[word].append(int(count))
                self.word_dense[word].append(int(page))
                
    def mapper_final(self):
        for word in self.word_dense:
            yield word, self.word_dense[word]
            
    def combiner(self, word, counts):
        counts = [c for c in counts]
        count = sum([c[0] for c in counts])
        page = sum([c[1] for c in counts])
        yield word, (count, page)
        
    def reducer(self, word, counts):
        counts = [c for c in counts]
        count = sum([c[0] for c in counts])
        page = sum([c[1] for c in counts])
        density = float(count/page)
        yield word, density    
    
    # END STUDENT CODE 5.6.1.C
        
if __name__ == '__main__':
    mostLeastDenseWords.run()
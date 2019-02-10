#!/opt/anaconda2/bin python

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRtopicPRPre(MRJob):
    def mapper(self, _, line):
        node, out_links = line.strip().split('\t')
        yield node, ast.literal_eval(out_links)

    def reducer(self, node, out_links):
        for link in out_links:
            if type(link) == dict:
                out_link = str(link)
            else:
                topic = str(link)
        yield node, out_link + "|" + topic

if __name__ == '__main__':
    MRtopicPRPre.run()
#!/opt/anaconda2/bin python

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep

import ast

class MRpageRank(MRJob):

    # Job will iterate, set option
    def steps(self):
        return (
                [MRStep(mapper = self.mapper_init)]
                +
                [MRStep(jobconf={
                    'mapreduce.job.reduces': '20',
                    'mapreduce.job.maps': '100',
               },
                        mapper = self.mapper_pagerank_outlinks,
                        reducer = self.reducer_pagerank_outlinks
                ),
                MRStep(jobconf={
                    'mapreduce.job.reduces': '20',
                },
                       reducer = self.reducer_update)
                ] * self.options.num_iterations
        )

    # Set number of iterations, number of nodes(total), dampening factor
    def configure_args(self):
        super(MRpageRank, self).configure_args()
        self.add_passthru_arg('--num-iterations', default=10, type=int, help='number of iterations to compute stable pagerank')
        self.add_passthru_arg('--nodes', default=None, type=float, help='total number of nodes')
        self.add_passthru_arg('--damp', default=0.85, type=float, help='dampening factor')

    # Initialize each node by emiting 1/G as a starting value for page rank
    def mapper_init(self, _, line):
        node, out_links = line.split('\t')
        pr = float(1) / self.options.nodes
        yield node, out_links + '|' + str(pr)

        
    # Distribute mass to the outgoing links
    def mapper_pagerank_outlinks(self, node, pr_out_links):
        # parse input to read page rank of the outgoing links
        out_links, pr = map(ast.literal_eval, pr_out_links.split('|'))
        
        # Simple page rank
        if len(out_links) > 0:
            pr_new = float(pr) / len(out_links)

            for out_link in out_links:
                yield out_link, pr_new

        # If the node is dangling, note for processing later
        if len(out_links) == 0:
            yield 'dangling', float(pr)

        # recover graph structure
        yield node, out_links
    
    def reducer_pagerank_outlinks(self, node, pr_out_links):

        # If the node is dangling, redistribute the loss to all the nodes in the graph
        if node == 'dangling':
            loss = sum(pr_out_links)
            for n in range(1, int(self.options.nodes) + 1):
                yield str(n), loss
                
        # Else combine the mass for the node
        else:
            M = 0
            out_links = {}

            for pr_out_link in pr_out_links:
                if type(pr_out_link) == dict:
                    out_links = pr_out_link
                elif type(pr_out_link) == float:
                    M += pr_out_link

            yield node, str(out_links) + '|' + str(M)
            
    def reducer_update(self, node, pr_out_links):
        loss = 0.0
        pr = 0.0
        out_links = {}
        
        # Teleportation factor
        a = 1 - self.options.damp
        N = self.options.nodes
        
        for pr_out_link in pr_out_links:
            if type(pr_out_link) == float:
                loss = pr_out_link
            else:
                out_links, pr = map(ast.literal_eval, pr_out_link.split('|'))

        pr_new = a * (float(1)/N) + (1-a) * (loss/float(N) + float(pr))

        yield node, str(out_links) + '|' + str(pr_new)

if __name__ == '__main__':
    MRpageRank.run()
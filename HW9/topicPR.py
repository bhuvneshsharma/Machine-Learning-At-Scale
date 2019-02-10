#!/opt/anaconda2/bin python

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep
import ast

class MRtopicPR(MRJob):

    TOPIC_COUNTS = 'rand_top_counts.out'
    
    def steps(self):
        return (
                [MRStep(mapper = self.mapper_init)] +
                [
                    MRStep(mapper = self.mapper_distribute, 
                           reducer = self.reducer_combine_mass),  
                    MRStep(reducer_init = self.topic_counts_init, 
                           reducer = self.reducer_update)
                ] * self.options.num_iterations
        )

    # Set options
    def configure_args(self):
        super(MRtopicPR, self).configure_args()
        self.add_passthru_arg('--num-iterations', default=10, type=int, help='number of iterations to compute stable pagerank')
        self.add_passthru_arg('--nodes', default=None, type=float, help='total number of webpages (or nodes)')
        self.add_passthru_arg('--damp', default=0.85, type=float, help='dampening factor')
        self.add_passthru_arg('--beta', default=0.99, type=float, help='beta irreducibility factor (closer to 1)')
        self.add_passthru_arg('--topics', default=10, type=int, help='number of topics')

    def topic_counts_init(self):
        self.topic_counts = {}
        for line in open(self.TOPIC_COUNTS).read().strip().split('\n'):
            line = line.split("\t")
            self.topic_counts[int(line[1])] = int(line[0])
        
    def mapper_init(self, _, line):
        node, topic_outlinks = line.split('\t')
        node = node.replace('"', '')
        topic_outlinks = topic_outlinks.replace('"', '')
        out_links, topic = topic_outlinks.split('|')
        pr = [ 1.0 / self.options.nodes ] * (self.options.topics + 1)

        yield node, out_links + "|" + topic + "|" + str(pr)

    # Distribute mass to outlinks
    def mapper_distribute(self, node, pr_out_links):
        out_links, topic, prs = map(ast.literal_eval, pr_out_links.split('|'))
        
        if len(out_links) > 0:
            pr_new = [ pr / len(out_links) for pr in prs ]
            for out_link in out_links:
                yield out_link, pr_new

        if len(out_links) == 0:
            yield 'dangling', prs

        yield node, pr_out_links

    # Update PR
    def reducer_combine_mass(self, node, pr_out_links):
        loss = [0] * (self.options.topics + 1)

        # Deal with dangling nodes
        if node == 'dangling':
            for dangling in pr_out_links:
                for l in range(len(dangling)):
                    loss[l] += dangling[l]

            for n in range(1, int(self.options.nodes) + 1):
                yield str(n), loss
    
        else:
            M = [0] * (self.options.topics + 1)
            out_links = {}
            topic = ""

            for pr_out_link in pr_out_links:
                if type(pr_out_link) == list:
                    for l in range(len(pr_out_link)):
                        M[l] += pr_out_link[l]
                else:
                    out_links, topic, prs = map(ast.literal_eval, pr_out_link.split('|'))

            yield node, str(out_links) + "|" + str(topic) + "|" + str(M)
            
    def reducer_update(self, node, pr_out_links):
        loss = [0] * (self.options.topics + 1)
        pr_new = []

        # Teleportation factor
        a = 1 - self.options.damp
        N = self.options.nodes
        b = self.options.beta

        for pr_out_link in pr_out_links:
            if len(pr_out_link) > 0:
                if type(pr_out_link) == list:
                    loss = pr_out_link
                else:
                    out_links, topic, pr = map(ast.literal_eval, pr_out_link.split('|'))

        # Update pagerank for each page in topic j
        for j in range(1, self.options.topics + 1):

            Tj = self.topic_counts[j]
            
            if j == topic:
                pr_new.append(a * (b/Tj) + (1-a) * (loss[j-1]/N + float(pr[j-1])))
            else:
                pr_new.append(a * ((1-b)/(N-Tj)) + (1-a) * (loss[j-1]/N + float(pr[j-1])))

        pr_new.append(a * (1/N) + (1-a) * (loss[j-1]/N + float(pr[j-1])))
                
        yield node, str(out_links) + "|" + str(topic) + "|" + str(pr_new)

if __name__ == '__main__':
    MRtopicPR.run()
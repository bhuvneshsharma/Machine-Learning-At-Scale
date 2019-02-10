#!/opt/anaconda2/bin python

from countNodes import MRcountNodes
import time
import sys

SOURCE = sys.argv[1]
RUNMODE = sys.argv[2]

start_time = time.time()
print 'processing file {}'.format(SOURCE)

mr_job = MRcountNodes(args=[SOURCE, '-r', RUNMODE, 
                            '--cmdenv', 'PATH=/opt/anaconda/bin:$PATH',
                            '--output-dir', '/user/lteo01/HW9/output',
                            '--no-output'])
with mr_job.make_runner() as runner:
    runner.run()
    nodes = []
    for line in runner.stream_output():
        nodes.append(int(line.split('\t')[1]))

    node_count = sum(nodes)

end_time = time.time()
print 'Time taken to do analysis = {:.2f} seconds'.format(end_time - start_time)

print 'Number of nodes = {}'.format(node_count)
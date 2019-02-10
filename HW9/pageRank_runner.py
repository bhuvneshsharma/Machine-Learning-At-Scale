#!/opt/anaconda/bin python

from pageRank import MRpageRank
import time
import sys
import subprocess as sp

outPATH = '/user/lteo01/HW9/output' # Altiscale
#outPATH = '/user/root/HW9/output' # docker
SOURCE = sys.argv[1]
RUNMODE = sys.argv[2]
ITER = sys.argv[3]
D = sys.argv[4]
N = sys.argv[5]


sp.Popen(['hadoop', 'fs', '-rm', '-r', outPATH], stdout=sp.PIPE)


start_time = time.time()
print 'processing file {}'.format(SOURCE)

mr_job = MRpageRank(args=[SOURCE, '-r', RUNMODE, 
                            '--num-iterations', ITER,
                            '--damp', D, '--nodes', N,
                            '--cmdenv', 'PATH=/opt/anaconda/bin:$PATH',
                            '--output-dir', outPATH,
                            '--no-output'])
with mr_job.make_runner() as runner:
    runner.run()
    proc = sp.Popen(['hadoop', 'fs', '-cat', outPATH + '/part-00000'], stdout=sp.PIPE)
    output = proc.communicate()[0].decode('utf-8')
    for line in output.splitlines():
        key, value =  mr_job.parse_output_line(line)

end_time = time.time()
print 'Time taken to do analysis = {:.2f} seconds'.format(end_time - start_time)
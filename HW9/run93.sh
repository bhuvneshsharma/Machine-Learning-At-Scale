!#/bin/bash


#time python ./pageRank_runner.py wikipedia/all-pages-indexed-out.txt hadoop 5 0.85 15192277
#hdfs dfs -cat /user/lteo01/HW9/output/* > test93_5.out

time python ./pageRank_runner.py wikipedia/all-pages-indexed-out.txt hadoop 10 0.85 15192277
hdfs dfs -cat /user/lteo01/HW9/output/* > test93_10.out

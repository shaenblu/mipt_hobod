#!/bin/bash
echo $@ | hadoop jar hadoop-streaming.jar -D mapreduce.job.reduces=1 -files mapper.py,reducer.py -input /data/stackexchange/posts -output ./output333 -mapper 'python mapper.py' -reducer 'python reducer.py';
echo $@ | hadoop jar hadoop-streaming.jar -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D stream.num.map.output.key.fields=2 -D mapreduce.partition.keycomparator.options=-k2,2nr -D mapreduce.job.reduces=1 -files mapper1.py,reducer1.py -input ./output333/part-00000 -output ./output1_ -mapper 'python mapper1.py' -reducer 'python reducer1.py'; 
hadoop fs -cat ./output1_/part-00000 | head -10;
rm -rf ./output333
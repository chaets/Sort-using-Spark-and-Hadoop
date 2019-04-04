#!/bin/bash

javac -classpath $(hadoop classpath) HadoopSort.java

jar cvf HadoopSort.jar *.class

#`hadoop jar HadoopSort.jar HadoopSort /input/data-20GB /user/cgupta5/output20G-hadoop -D dfs.blocksize=128m -D mapreduce.job.reduces=4 -D mapreduce.task.io.sort.mb=1024`
START_TIME=$SECONDS
hadoop jar HadoopSort.jar HadoopSort -D mapreduce.job.reduces=8 /input/data-20GB /user/cgupta5/output20G-hadoop
ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "VALIDATING THE OUTPUT..."

hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/cgupta5/output20G-hadoop /user/cgupta5/report-hadoop

hadoop fs -get /user/cgupta5/report-hadoop/part-r-00000

echo "printing checksum using part-r-00000..."

cat part-r-00000

rm part-r-00000


echo -e "Time taken: $ELAPSED_TIME secs"

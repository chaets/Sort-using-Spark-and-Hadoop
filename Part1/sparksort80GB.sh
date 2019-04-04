#!/bin/bash


# Compilation and Creation SparkSort

javac -classpath /opt/spark-2.3.0-bin-hadoop2.7/jars/spark-core_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/spark-sql_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-compiler-2.11.8.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-library-2.11.8.jar SparkSort.java

jar cvf SparkSort.jar *.class


START_TIME=$SECONDS
spark-submit --class SparkSort --master yarn --deploy-mode client --driver-memory 1g --executor-memory 1g --executor-cores 1 --num-executors 1 SparkSort.jar /input/data-80GB /user/cgupta5/output-spark


ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "VALIDATING THE OUTPUT..."

#hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/cgupta5/output-spark /user/cgupta5/report-spark

#hadoop fs -get /user/cgupta5/report-spark/part-r-00000

#cat part-r-00000


echo -e "Time taken: $ELAPSED_TIME secs"

#!/bin/bash

HADOOP_CONF_DIR=/etc/hadoop/conf/
YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

##
#  spark-shell --master yarn --jars jbtct/ct-0.0.1.jar jbtct/breeze_2.11-0.10.jar --queue testing --num-executors 100
#  spark-shell --deploy-mode client --master local[*] --jars ../../ct-0.0.1.jar ../../breeze_2.11-0.10.jar --conf spark.executor.extraJavaOptions=-XX:-UseConcMarkSweepGC
##
$class=org.jobimtext.run

# testing, shortrunning, longrunnning, default
queue=testing

num_executors=100
mem_driver=2g
mem_executor=1g

jarfile=sparkfun-retroed.jar

params="${1} ${2}"

spark-submit \
--class=$class \
--master=yarn-cluster \
--queue=$queue \
--num-executors $num_executors \
--driver-memory $mem_driver \
--executor-memory $mem_executor \
$jarfile \
$params
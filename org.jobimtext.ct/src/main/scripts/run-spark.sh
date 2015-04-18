#!/bin/bash

HADOOP_CONF_DIR=/etc/hadoop/conf/
YARN_CONF_DIR=/etc/hadoop/conf.cloudera.yarn/

##
#  spark-shell --master yarn --jars jbtct/ct-0.0.1.jar jbtct/breeze_2.11-0.10.jar --queue testing --num-executors 100
#  spark-shell --deploy-mode client --master local[*] --jars ../../ct-0.0.1.jar ../../breeze_2.11-0.10.jar --conf spark.executor.extraJavaOptions=-XX:-UseConcMarkSweepGC
#  spark-submit --jars jbtct/breeze_2.11-0.10.jar --class org.jobimtext.run.ExtractRunner --master=yarn-cluster --queue=testing --num-executors 100 jbtct/ct-0.0.1-lt.jar --in=wiki.en.simple/sents --out=wiki.en.simple/coocs --windowsize=3
#  spark-submit --jars jbtct/breeze_2.11-0.10.jar,jbtct/ct-0.0.1-lt.jar --files jbtct/ct.log4j.properties --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:ct.log4j.properties" --class org.jobimtext.run.ExtractRunner --master yarn-cluster --queue testing --num-executors 100 --verbose ct-0.0.1-lt.jar -in wiki.en.simple/sent -out wiki.en.simple/coocs -windowsize 5
#  lib/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --jars breeze_2.11-0.10.jar,ct-0.0.1.jar --files spark.log4j.properties --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:spark.log4j.properties" --class org.jobimtext.run.ExtractRunner --master local[3] --num-executors 100 --verbose ct-0.0.1.jar -in data/wiki.en.simple/simplewikipedia_sent_tok.txt -out tmp/wiki.en.simple.coocs -windowsize 5
#  lib/spark-1.3.0-bin-hadoop2.4/bin/spark-submit --driver-memory 2g --executor-memory 2g --jars breeze_2.11-0.10.jar,ct-0.0.1.jar --files spark.log4j.properties --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:spark.log4j.properties" --class org.jobimtext.run.KLRunner1 --master local[*] --num-executors 32 --verbose ct-0.0.1.jar -in tmp/wiki.en.simple.coocs -out tmp/wiki.en.simple.coocs_sim -checkpoint true -topnf 1000 -sort true
#   
# kyroserializer
# defaultparallelism
# vm.max_map_count to 12800000 (os setting) (https://my.vertica.com/docs/4.1/HTML/Master/12962.htm)
#  
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
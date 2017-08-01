#!/bin/bash

#Created by turk 2015-08-13
#Project Apollo V1 PortDetect

WORKPATH=/home/hadoop/turk/
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apollo_evaluate.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----Project Apollo V1 Evaluate-----" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-09-24)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE



ZOOSERVER=hadoop-r720-1

#spark-submit --master yarn-cluster --class "xxx.YourApp" --conf "spark.driver.extraJavaOptions=-Dspring.profiles.active=production" --num-executors 6 --driver-memory 4g --executor-memory 8g --executor-cores 6 --conf "spark.executor.extraJavaOptions=-Dspring.profiles.active=production" --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/*" --driver-class-path "/opt/cloudera/parcels/CDH/lib/hbase/lib/*" yourAppJarFile.jar -Dlog4j.configuration=log4j.properties

spark-submit --class "com.aotain.project.apollo.EvaluateCaluSite" --master yarn --num-executors 4 --driver-memory 4G --executor-memory 8G --conf "spark.executor.extraJavaOptions=-Dspring.profiles.active=production" --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar" --driver-class-path "/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar" --conf "spark.log.file=evaluate" --jars ${JAVALIB}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${JAVALIB}/Hades-0.0.1-SNAPSHOT.jar $ZOOSERVER


echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0


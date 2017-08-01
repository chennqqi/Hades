#!/bin/bash

#Created by turk 2015-09-18
#Project Apollo V1 Reverse Detect.

WORKPATH=/home/hadoop/turk/
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apollo_reversedetect.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Project Apollo V1 Reverse Detect--------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-09-15)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


ZOOSERVER=hadoop-r720-1,hadoop-r720-2,hadoop-r720-3

#spark-submit --master yarn-cluster --class "xxx.YourApp" --conf "spark.driver.extraJavaOptions=-Dspring.profiles.active=production" --num-executors 6 --driver-memory 4g --executor-memory 8g --executor-cores 6 --conf "spark.executor.extraJavaOptions=-Dspring.profiles.active=production" --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/*" --driver-class-path "/opt/cloudera/parcels/CDH/lib/hbase/lib/*" yourAppJarFile.jar -Dlog4j.configuration=log4j.properties

spark-submit --class "com.aotain.project.apollo.ReverseDetect" --master yarn --num-executors 4 --driver-memory 2G --executor-memory 4G --conf "spark.executor.extraJavaOptions=-Dspring.profiles.active=production" --conf "spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar" --driver-class-path "/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core-3.1.0-incubating.jar" --conf "spark.log.file=reverse" --jars ${JAVALIB}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${JAVALIB}/Hades-0.0.1-SNAPSHOT.jar $ZOOSERVER

echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0


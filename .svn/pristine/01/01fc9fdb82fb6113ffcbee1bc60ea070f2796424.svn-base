#!/bin/bash

#Created by turk 2015-07-16
#Project Apollo V1 IPParis

WORKPATH=/home/bsmp/work/
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apollo_ipparis.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Project Apollo V1 IPParis--------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-07-16)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><DAY>"  | tee -a $LOGFILE
	exit 1
fi


spark-submit --class com.aotain.project.apollo.IPParisML --master yarn --num-executors 16 --jars ${JAVALIB}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${JAVALIB}/Hades-0.0.1-SNAPSHOT.jar $1 $2> $LOGFILE 2>&1


echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0


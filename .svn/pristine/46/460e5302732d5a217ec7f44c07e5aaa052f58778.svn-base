#!/bin/bash

#Created by turk 2015-08-05
#Version 1.0 



#if [ $# -lt 2 ]; then
#	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<TABLE1> <TABLE2>"
#	exit 1
#fi

#TABLE1=$1
#TABLE2=$2

WORKPATH=/home/bsmp/work/
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apllo_s1_idcdata.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-08-05)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Apollo S1"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE



spark-submit --class com.aotain.project.apollo.IDCDataTest --master yarn --num-executors 16 --jars ${JAVALIB}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${JAVALIB}/Hades-0.0.1-SNAPSHOT.jar $1 $2 > $LOGFILE 2>&1


echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0

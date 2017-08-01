#!/bin/bash

#Created by turk 2015-07-10
#Domain 
#Version 1.0 



if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<TABLE1> <TABLE2>"
	exit 1
fi

TABLE1=$1
TABLE2=$2

WORKPATH=/home/bsmp/work/
JAVALIB=${WORKPATH}/mr/
LOGFILE=$WORKPATH/log/applo_S1.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-07-10)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Apllo S1"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


#HDFS--

#HDFS--end

#hdfs mapreduce
spark-submit --class com.aotain.spark.DPIAbnormalLog --master yarn --num-executors 16 --jars ${WORKPATH}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${WORKPATH}/Hades-0.0.1-SNAPSHOT.jar

#mapreduce
hadoop jar $JAVALIB/Hades-1.1.jar com.aotain.dw.DomainStatDriver ${DATAPATH} ${OUTPUTPATH} ${REMARK} 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi
echo `date +"%Y-%m-%d %H:%M:%S"`      "Domain stat. Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


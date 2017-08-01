#!/bin/bash

#Created by turk 2015-07-14
#Domain 
#Version 1.0 



if [ $# -lt 3 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<TABLENAME> <STARTROW> <ENDROW>"
	exit 1
fi

TABLENAME=$1
STARTROW=$2
ENDROW=$3

WORKPATH=/home/bsmp/work/
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/hbase_export_sh.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-07-14)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Hbase Export"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE




java -cp ${JAVALIB}/Hades-0.0.1-SNAPSHOT-jar-with-dependencies.jar:${JAVALIB}/Hades-0.0.1-SNAPSHOT.jar:${JAVALIB}/log4j-1.2.17.jar:${WORK}/config/ -Dflume.log.file=HbaseData com.aotain.hbase.dataoutput.HBaseTestCase ${TABLENAME} ${STARTROW} ${ENDROW} > $LOGFILE  2>&1
echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0

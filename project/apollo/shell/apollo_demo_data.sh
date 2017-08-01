#!/bin/bash

#Created by turk 2015-09-09
#Version 1.0 



if [ $# -lt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<IP>"
	exit 1
fi

IP=$1

WORKPATH=/home/hadoop/turk
DATAPATH=/data/hadoop/data/buff
DATAOUTPUT=/data/hadoop/output

JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apollo_demo.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-09-09)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Apollo Demo Data"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


FILENUM=`ls ${DATAPATH}/*.txt | wc -l`
echo ${FILENUM}
if [ $FILENUM -gt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"` "Start Grep" | tee -a $LOGFILE
	for num in $( seq 1 $FILENUM )
	do
	 FILENAME=`ls ${DATAPATH}/*.txt | sed -n "$num"p`
		echo `date +"%Y-%m-%d %H:%M:%S"` "FILENAME:${FILENAME}" | tee -a $LOGFILE
		cat ${FILENAME} | grep "${IP}" > ${DATAOUTPUT}/${IP}_$num.txt
	done
fi


echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0

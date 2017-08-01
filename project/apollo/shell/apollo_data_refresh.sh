#!/bin/bash

#Created by turk 2015-09-09
#Version 1.0 



#if [ $# -lt 2 ]; then
#	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<TABLE1> <TABLE2>"
#	exit 1
#fi

WORKPATH=/home/hadoop/turk
DATAPATH=/data/hadoop/turk
JAVALIB=${WORKPATH}/lib/
LOGFILE=${WORKPATH}/log/apollo_data_refresh.log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-09-09)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Apollo Data Refresh"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


FILENUM=`ls ${DATAPATH}/flumebuff/*.txt | wc -l`

if [ $FILENUM -lt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"` "Start tar zxvf" | tee -a $LOGFILE
	cd ${DATAPATH}
	for tar in ${DATAPATH}/*.gz;do tar zxvf $tar -C ${DATAPATH};done > $LOGFILE 2>&1
	rm -rf ${DATAPATH}/flumebuff/*.COMPLETED
	mv ${DATAPATH}/buff/*.txt ${DATAPATH}/flumebuff/
	echo `date +"%Y-%m-%d %H:%M:%S"` "END tar zxvf" | tee -a $LOGFILE
fi

DATAPATH2=/data/hadoop/101.227.160.27

FILENUM2=`ls ${DATAPATH2}/flumebuff/ | wc -l`
if [ $FILENUM2 -lt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"` "Start tar zxvf" | tee -a $LOGFILE
	cd ${DATAPATH2}
	for tar in ${DATAPATH2}/*.gz;do tar zxvf $tar -C ${DATAPATH2};done > $LOGFILE 2>&1
	rm -rf ${DATAPATH2}/flumebuff/*.COMPLETED
	mv ${DATAPATH2}/output/*.txt ${DATAPATH2}/flumebuff/
	echo `date +"%Y-%m-%d %H:%M:%S"` "END tar zxvf" | tee -a $LOGFILE
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Quit" | tee -a $LOGFILE

exit 0

#!/bin/bash

#Created by turk 2015-01-07
#Terminal Data Cleaning
#Version 1.0  
#Version 1.1  modified by turk 2015-01-29 add cityname



if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/terminal
JAVALIB=${WORKPATH}/mr/
LOGFILE=$WORKPATH/log/Terminal_Data_Cleaning_${CITYNAME}_"$DATE".log
MRJAR=${JAVALIB}/terminal.jar


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.1 (2015-01-29)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Terminal Data Cleaning"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE INPUTPATH:$INPUTPATH OUTPUTPATH:$OUTPUTPATH"   | tee -a $LOGFILE

#HDFS--
INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/${CITYNAME}/
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/tw_terminal_info/${CITYNAME}/

DATAPATH="$INPUTPATH"/"$DATE"
OUTPUTPATH="$OUTPUTPATH"/"$DATE"
#HDFS--end

#hdfs mapreduce
echo "DATA PATH:" $DATAPATH | tee -a $LOGFILE
echo "OUTPUT PATH:" $OUTPUTPATH | tee -a $LOGFILE

#mapreduce
#hadoop jar $JAVALIB/Hades-1.1.jar com.aotain.spark.TerminalDC $DATAPATH $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
#if [ $? -ne 0 ]; then
#        exit $?
#fi

#spark
spark-submit --class com.aotain.ods.TerminalDC --deploy-mode cluster --master yarn --num-executors 480 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} $DATAPATH/*.lzo $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
if [ $? -ne 0 ]; then
        exit $?
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Success!" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`      "Data Cleaning Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


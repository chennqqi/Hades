#!/bin/bash

#Created by turk 2015-01-28
#User Last Access URL
#Version 1.0  



if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bsmp/work/
JAVALIB=/home/bsmp/work/mr/
LOGFILE=$WORKPATH/log/UserLastUrl_"$CITYNAME"_"$DATE".log


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-01-28)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "User Last Access URL"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE"   | tee -a $LOGFILE

#HDFS--
INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/"$CITYNAME"
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/tw_user_lasturl/"$CITYNAME"

DATAPATH="$INPUTPATH"/"$DATE"
OUTPUTPATH="$OUTPUTPATH"/"$DATE"
#HDFS--end

#hdfs mapreduce
echo "DATA PATH:" $DATAPATH | tee -a $LOGFILE
echo "OUTPUT PATH:" $OUTPUTPATH | tee -a $LOGFILE

#mapreduce
hadoop jar $JAVALIB/Hades-1.1.jar com.aotain.hdfs.mr.UserLastUrlDriver $DATAPATH $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
if [ $? -ne 0 ]; then
        exit $?
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Success!" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


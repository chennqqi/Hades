#!/bin/bash

#Created by wayne 2014-12-14
#clean post data
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=to_opr_post
MRCLASS=com.aotain.ods.PostDriver
INPUTPATH=/user/hive/warehouse/broadband.db/post/$CITY/$DATE
OUTPUTPATH=/user/hive/warehouse/broadband.db/$TABLENAME/$CITY/$DATE
CONFIG=/home/bigdata/project/ods/config/$TABLENAME.xml
BAKPATH=/user/hive/warehouse/aotain_bak

MRPATH=/home/bigdata/project/ods/mr
LOGFILE=/home/bigdata/project/ods/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hadoop fs -mkdir /user/hive/warehouse/broadband.db/post/$CITY 1>>$LOGFILE 2>>$LOGFILE
hadoop fs -mkdir $INPUTPATH 1>>$LOGFILE 2>>$LOGFILE
hive -e "use broadband;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hadoop jar $MRPATH/ods.jar $MRCLASS $INPUTPATH $OUTPUTPATH $CONFIG $DATE 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	exit $ret
fi

#rmdata
DATE=`date -d "7 day ago" +%Y%m%d`
hadoop fs -rm -r /user/hive/warehouse/broadband.db/post/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE 

exit 0

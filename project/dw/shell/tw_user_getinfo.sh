#!/bin/bash

#Created by wayne 2014-12-14
#create a history table for httpget k-v data
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tw_user_getinfo
MRCLASS=com.aotain.dw.UserAttDriver
INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/$CITY/$DATE
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY
CONFIG=/home/bigdata/project/dw/config/$TABLENAME.xml
BAKPATH=/user/hive/warehouse/aotain_bak/"$TABLENAME"_"$CITY"_"$DATE"

MRPATH=/home/bigdata/project/dw/mr
LOGFILE=/home/bigdata/project/dw/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hadoop fs -mkdir $BAKPATH 1>>$LOGFILE 2>>$LOGFILE 1>>$LOGFILE 2>>$LOGFILE
hadoop fs -mkdir /user/hive/warehouse/broadband.db/to_opr_http/$CITY 1>>$LOGFILE 2>>$LOGFILE
hadoop fs -mkdir /user/hive/warehouse/broadband.db/to_opr_http/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE
hive -e "use aotain_dw;alter table $TABLENAME add partition(citycode='$CITY') location '$CITY';" 1>>$LOGFILE 2>>$LOGFILE

#copy data
hadoop fs -mv $OUTPUTPATH/* $BAKPATH 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hadoop jar $MRPATH/dw.jar $MRCLASS $INPUTPATH"|lzo#"$BAKPATH"|txt" $OUTPUTPATH $CONFIG $DATE 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	hadoop fs -mv $BAKPATH/* $OUTPUTPATH
        exit $ret
fi

#rmdata
DATE=`date -d "3 day ago" +%Y%m%d`
hadoop fs -rm -r /user/hive/warehouse/aotain_bak/"$TABLENAME"_"$CITY"_"$DATE" 1>>$LOGFILE 2>>$LOGFILE

exit 0

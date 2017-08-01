#!/bin/bash

#Created by wayne 2014-12-14
#split ip region for areanetwork data
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "2 day ago" +%Y%m%d`
fi

TABLENAME=to_ref_areanetwork
MRCLASS=com.aotain.dim.AreaUserParseDriver
INPUTPATH=/user/hive/warehouse/broadband.db/areanetwork/$CITY/$DATE
OUTPUTPATH=/user/hive/warehouse/aotain_dim.db/$TABLENAME/$CITY/$DATE

MRPATH=/home/bigdata/project/dim/mr
LOGFILE=/home/bigdata/project/dim/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hadoop fs -mkdir /user/hive/warehouse/broadband.db/areanetwork/$CITY 1>>$LOGFILE 2>>$LOGFILE
hadoop fs -mkdir /user/hive/warehouse/broadband.db/areanetwork/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE
hive -e "use aotain_dim;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hadoop fs -test -e $INPUTPATH
if [ $? -eq 0 ] ;then
  hadoop jar $MRPATH/dim.jar $MRCLASS $INPUTPATH $OUTPUTPATH $DATE 1>>$LOGFILE 2>>$LOGFILE
  ret=$?
  if [ $ret -ne 0 ]; then
        exit $ret
  fi
fi

#rmdata
DATE=`date -d "7 day ago" +%Y%m%d`
hadoop fs -rm -r /user/hive/warehouse/broadband.db/areanetwork/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE

exit 0

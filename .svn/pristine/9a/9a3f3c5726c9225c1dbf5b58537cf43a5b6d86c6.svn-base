#!/bin/bash

#Created by wayne 2014-12-14
#clean app data
#Version 1.0  modified by wayne 2015-05-18

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=to_opr_app
OUTPUTPATH=/user/hive/warehouse/broadband.db/$TABLENAME/$CITY/$DATE

LOGFILE=/home/bigdata/project/ods/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hive -e "use broadband;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#rm data
hadoop fs -rm $OUTPUTPATH/*

#mapreduce
hive -e "set mapred.job.name=$TABLENAME_$CITY_$DATE;
insert overwrite directory '${OUTPUTPATH}'
select concat_ws('|',cast(b.starttime as string),
cast(b.endtime as string),
b.apptype,
b.appid,
b.appname,
b.username,
cast(b.apptraffic_up as string),
cast(b.apptraffic_dn as string),
cast(b.apppacketsnum as string),
cast(b.appsessionsnum as string),
cast(b.appnewsessionnum as string)) from aotain_dm_terminalstat.tm_static_userinfo a join broadband.appuser b
on a.userid = b.username where a.citycode='$CITY' and b.partdate='$DATE';" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	exit $ret
fi

#rmdata
DATE=`date -d "7 day ago" +%Y%m%d`
hive -e "use broadband;alter table $TABLENAME drop partition(citycode='$CITY',partdate='$DATE');"
hadoop fs -rm -r /user/hive/warehouse/broadband.db/$TABLENAME/$CITY/$DATE

exit 0

#!/bin/bash

#Created by wayne 2014-12-14
#user attribute histort table
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago"   +%Y%m%d`
fi

TABLENAME=tm_static_userinfo
MRCLASS=com.aotain.project.terminal.UserStaticInfoMR
INPUTPATH=/user/hive/warehouse/aotain_dw.db/tw_user_postinfo/$CITY#/user/hive/warehouse/aotain_dw.db/tw_user_getinfo/$CITY#/user/hive/warehouse/aotain_dw.db/tw_user_appinfo/$CITY
OUTPUTPATH=/user/hive/warehouse/aotain_dm_terminalstat.db/$TABLENAME/$CITY
CONFIG=/home/bigdata/project/terminal/config/$TABLENAME.xml
BAKPATH=/user/hive/warehouse/aotain_bak

MRPATH=/home/bigdata/project/terminal/mr
LOGFILE=/home/bigdata/project/terminal/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hive -e "use aotain_dm_terminalstat;alter table $TABLENAME add partition(citycode='$CITY') location '$CITY';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hadoop jar $MRPATH/terminal.jar $MRCLASS $INPUTPATH $OUTPUTPATH $CONFIG $DATE $CITY 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

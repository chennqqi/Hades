#!/bin/bash

#Created by liujq 2015-04-20
#smart_push push_ad_area_rpt

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/bsmp/work/lib/ojdbc14.jar
TABLENAME=push_ad_area_rpt
OUTPUTPATH=/user/hive/warehouse/aotain_dm_push.db/$TABLENAME
LOGFILE=/home/bigdata/project/push/log/"$TABLENAME"_"$DATE".log

IP=202.104.1.48
SERVICENAME=bsmp
USERNAME=green_push24
PASSWD=green_push24168


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME --columns  AD_ID,ADAR_AREAID,ADAR_STATCYCLE,ADAR_STATTIME,ADAR_PUSHTIMES,ADAR_PUSHUSER,ADAR_PUSHIP,ADAR_ARRIVETIMES,ADAR_ARRIVEUSER,ADAR_ARRIVEIP,ADAR_CLICKTIMES,ADAR_CLICKUSER,ADAR_SUBMITUSER,ADAR_REGISTERUSER,ADAR_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001'  -m 2 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

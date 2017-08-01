#!/bin/bash

#Created by liujq 2015-04-20
#smart_push push_ad_site_rpt

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/bsmp/work/lib/ojdbc14.jar
TABLENAME=push_ad_site_rpt
OUTPUTPATH=/user/hive/warehouse/aotain_dm_push.db/$TABLENAME
LOGFILE=/home/bigdata/project/push/log/"$TABLENAME"_"$DATE".log

IP=116.55.230.126
SERVICENAME=bsmp
USERNAME=newpush
PASSWD=newpush168


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME --columns AD_ID,ADSI_STATDAY,ADSI_DOMAIN,ADSI_PUSHTIMES,ADSI_PUSHUSER,ADSI_ARRIVETIMES,ADSI_ARRIVEUSER,ADSI_CLICKTIMES,ADSI_CLICKUSER,ADSI_SUBMITUSER,ADSI_REGISTERUSER,ADSI_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001' -m 2 --input-null-string '\\N'  --input-null-non-string '\\N'  1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

#!/bin/bash

#Created by liujq 2015-04-20
#smart_push push_task_day_stat_rpt

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/bsmp/work/lib/ojdbc14.jar
TABLENAME=push_task_day_stat_rpt
OUTPUTPATH=/user/hive/warehouse/aotain_dm_push.db/$TABLENAME
LOGFILE=/home/bigdata/project/push/log/"$TABLENAME"_"$DATE".log


IP=116.55.230.126
SERVICENAME=bsmp
USERNAME=newpush
PASSWD=newpush168

Push=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipspush/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print $7","$8}
' | sort | uniq`

Feedback=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipsfeedback/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print
$7","$8}' | sort | uniq`

Flowguide=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipsflowguide/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")prin
t $7","$8}' | sort | uniq`


if ["$Push" = ""] && ["$Feedback" = ""] && ["$Flowguide" = ""];then
   exit 0
fi

#rm data
hadoop fs -rm ${OUTPUTPATH}/* 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name = push_taskdaystat_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select taskid,tada_statday,areaid,0,0,sum(tada_flowguidetimes) tada_flowguidetimes,sum(tada_flowguideuser) tada_flowguideuser,
sum(tada_pushtimes) tada_pushtimes,sum(tada_pushuser) tada_pushuser,
sum(tada_arrivetimes) tada_arrivetimes,sum(tada_arriveuser) tada_arriveuser,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') tada_createtime
from (
select taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') tada_statday,
count(username) tada_pushtimes,count(distinct username) tada_pushuser,
0 tada_arrivetimes,0 tada_arriveuser,0 tada_flowguidetimes,0 tada_flowguideuser
from broadband.push_pushlog where partdate='${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') = '${DATE}' 
group by taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') tada_statday,
0 tada_pushtimes ,0 tada_pushuser,count(username) tada_arrivetimes,
count(distinct username) tada_arriveuser,0 tada_flowguidetimes,0 tada_flowguideuser
from broadband.push_feedback where partdate= '${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') = '${DATE}' 
and feedbacktype=1 group by taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') tada_statday,
0 tada_pushtimes ,0 tada_pushuser,0 tada_arrivetimes,0 tada_arriveuser,
count(username) tada_flowguidetimes,count(distinct username) tada_flowguideuser
from broadband.push_flowguide where partdate= '${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') = '${DATE}' 
group by taskid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
) temp  where areaid is not null and tada_statday is not null
group by taskid,tada_statday,areaid;" 1>>$LOGFILE 2>>$LOGFILE


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME  --columns  TASK_ID,TAST_STATDAY,TADA_AREAID,TADA_TOTALUSER,TADA_ONLINEUSER,TADA_FLOWGUIDETIMES,TADA_FLOWGUIDEUSER,TADA_PUSHTIMES,TADA_PUSHUSER,TADA_ARRIVETIMES,TADA_ARRIVEUSER,TADA_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001'  -m 2 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

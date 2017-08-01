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

IP=116.55.230.126
SERVICENAME=bsmp
USERNAME=newpush
PASSWD=newpush168

Push=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipspush/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print $7","$8}
' | sort | uniq`

Feedback=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipsfeedback/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print
$7","$8}' | sort | uniq`

if ["$Push" = ""] && ["$Feedback" = ""];then
   exit 0
fi

#rm data
hadoop fs -rm ${OUTPUTPATH}/* 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name = push_adarea_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select adid,areaid,adar_statcycle,adar_starttime,
sum(adar_pushtimes) adar_pushtimes,sum(adar_pushuser) adar_pushuser,sum(adar_puship) adar_puship,
sum(adar_arrivetimes) adar_arrivetimes,sum(adar_arriveuser) adar_arriveuser,sum(adar_arriveip) adar_arriveip,
sum(adar_clicktimes) adar_clicktimes,sum(adar_clickuser) adar_clickuser,
sum(adar_submituser) adar_submituser,sum(adar_registeruser) adar_registeruser,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') adar_createtime
from (
select adid,areaid,2 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adar_starttime,
count(username) adar_pushtimes,count(distinct username) adar_pushuser,count(distinct strip) adar_puship,
0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,0 adar_registeruser
from broadband.push_pushlog where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,areaid,2 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,
count(username) adar_arrivetimes,count(distinct username) adar_arriveuser,count(distinct strip) adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=1 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,areaid,2 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
count(username) adar_clicktimes,count(distinct username) adar_clickuser,0 adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=2 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,areaid,2 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,count(distinct username) adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}' 
and feedbacktype=3 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,areaid,2 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,count(distinct username) adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=4 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') 
union all
select adid,areaid,1 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adar_starttime,
count(username) adar_pushtimes,count(distinct username) adar_pushuser,count(distinct strip) adar_puship,
0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,0 adar_registeruser
from broadband.push_pushlog where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all
select adid,areaid,1 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,
count(username) adar_arrivetimes,count(distinct username) adar_arriveuser,count(distinct strip) adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=1 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all	
select adid,areaid,1 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
count(username) adar_clicktimes,count(distinct username) adar_clickuser,0 adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=2 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all	
select adid,areaid,1 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,count(distinct username) adar_submituser,0 adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}' 
and feedbacktype=3 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all
select adid,areaid,1 adar_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adar_starttime,
0 adar_pushtimes ,0 adar_pushuser,0 adar_puship,0 adar_arrivetimes,0 adar_arriveuser,0 adar_arriveip,
0 adar_clicktimes,0 adar_clickuser,0 adar_submituser,count(distinct username) adar_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=4 group by adid,areaid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
) temp	where  areaid is not null and adar_statcycle is not null and adar_starttime is not null
group by adid,areaid,adar_statcycle,adar_starttime;" 1>>$LOGFILE 2>>$LOGFILE


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME --columns  AD_ID,ADAR_AREAID,ADAR_STATCYCLE,ADAR_STATTIME,ADAR_PUSHTIMES,ADAR_PUSHUSER,ADAR_PUSHIP,ADAR_ARRIVETIMES,ADAR_ARRIVEUSER,ADAR_ARRIVEIP,ADAR_CLICKTIMES,ADAR_CLICKUSER,ADAR_SUBMITUSER,ADAR_REGISTERUSER,ADAR_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001'  -m 2 --input-null-string '\\N'  --input-null-non-string '\\N' 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

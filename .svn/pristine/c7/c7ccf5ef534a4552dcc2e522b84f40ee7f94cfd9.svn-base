#!/bin/bash

#Created by liujq 2015-04-20
#smart_push push_ad_time_rpt

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/bsmp/work/lib/ojdbc14.jar
TABLENAME=push_ad_time_rpt
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
hive -e "set mapred.job.name = push_adtime_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select adid,adti_statcycle,adti_starttime,
sum(adti_pushtimes) adti_pushtimes,sum(adti_pushuser) adti_pushuser,sum(adti_puship) adti_puship,
sum(adti_arrivetimes) adti_arrivetimes,sum(adti_arriveuser) adti_arriveuser,sum(adti_arriveip) adti_arriveip,
sum(adti_clicktimes) adti_clicktimes,sum(adti_clickuser) adti_clickuser,
sum(adti_submituser) adti_submituser,sum(adti_registeruser) adti_registeruser,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') adti_createtime
from (
select adid,2 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adti_starttime,
count(username) adti_pushtimes,count(distinct username) adti_pushuser,count(distinct strip) adti_puship,
0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,0 adti_registeruser
from broadband.push_pushlog where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,2 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,
count(username) adti_arrivetimes,count(distinct username) adti_arriveuser,count(distinct strip) adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=1 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,2 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
count(username) adti_clicktimes,count(distinct username) adti_clickuser,0 adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=2 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,2 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,count(distinct username) adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}' 
and feedbacktype=3 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,2 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,count(distinct username) adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=4 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') 
union all
select adid,1 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adti_starttime,
count(username) adti_pushtimes,count(distinct username) adti_pushuser,count(distinct strip) adti_puship,
0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,0 adti_registeruser
from broadband.push_pushlog where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all
select adid,1 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,
count(username) adti_arrivetimes,count(distinct username) adti_arriveuser,count(distinct strip) adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=1 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all	
select adid,1 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
count(username) adti_clicktimes,count(distinct username) adti_clickuser,0 adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=2 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all	
select adid,1 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,count(distinct username) adti_submituser,0 adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}' 
and feedbacktype=3 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
union all
select adid,1 adti_statcycle,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH') adti_starttime,
0 adti_pushtimes ,0 adti_pushuser,0 adti_puship,0 adti_arrivetimes,0 adti_arriveuser,0 adti_arriveip,
0 adti_clicktimes,0 adti_clickuser,0 adti_submituser,count(distinct username) adti_registeruser 
from broadband.push_feedback where partdate='${DATE}'  and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=4 group by adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMddHH')
) temp where adti_statcycle is not null and adti_starttime is not null
group by adid,adti_statcycle,adti_starttime;" 1>>$LOGFILE 2>>$LOGFILE


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME  --columns  AD_ID,ADTI_STATCYCLE,ADTI_STATTIME,ADTI_PUSHTIMES,ADTI_PUSHUSER,ADTI_PUSHIP,ADTI_ARRIVETIMES,ADTI_ARRIVEUSER,ADTI_ARRIVEIP,ADTI_CLICKTIMES,ADTI_CLICKUSER,ADTI_SUBMITUSER,ADTI_REGISTERUSER,ADTI_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001'  -m 2 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

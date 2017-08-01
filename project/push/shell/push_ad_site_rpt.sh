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
hive -e "set mapred.job.name = push_adsite_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select adid,adsi_statday,adsi_domain,
sum(adsi_pushtimes) adsi_pushtimes,sum(adsi_pushuser) adsi_pushuser,
sum(adsi_arrivetimes) adsi_arrivetimes,sum(adsi_arriveuser) adsi_arriveuser,
sum(adsi_clicktimes) adsi_clicktimes,sum(adsi_clickuser) adsi_clickuser,
sum(adsi_submituser) adsi_submituser,sum(adsi_registeruser) adsi_registeruser,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') adsi_createtime
from (
select adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adsi_statday,url adsi_domain,
count(username) adsi_pushtimes,count(distinct username) adsi_pushuser,
0 adsi_arrivetimes,0 adsi_arriveuser,0 adsi_clicktimes,0 adsi_clickuser,0 adsi_submituser,0 adsi_registeruser
from broadband.push_pushlog where partdate= '${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') = '${DATE}'
group by adid,url,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adsi_statday,url adsi_domain,
0 adsi_pushtimes ,0 adsi_pushuser,count(username) adsi_arrivetimes,count(distinct username) adsi_arriveuser,
0 adsi_clicktimes,0 adsi_clickuser,0 adsi_submituser,0 adsi_registeruser 
from broadband.push_feedback where partdate='${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=1 group by adid,url,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adsi_statday,url adsi_domain,
0 adsi_pushtimes ,0 adsi_pushuser,0 adsi_arrivetimes,0 adsi_arriveuser,
count(username) adsi_clicktimes,count(distinct username) adsi_clickuser,0 adsi_submituser,0 adsi_registeruser 
from broadband.push_feedback where partdate='${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=2 group by adid,url,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all	
select adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adsi_statday,url adsi_domain,
0 adsi_pushtimes ,0 adsi_pushuser,0 adsi_arrivetimes,0 adsi_arriveuser,
0 adsi_clicktimes,0 adsi_clickuser,count(distinct username) adsi_submituser,0 adsi_registeruser 
from broadband.push_feedback where partdate='${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=3 group by adid,url,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
union all
select adid,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') adsi_statday,url adsi_domain,
0 adsi_pushtimes ,0 adsi_pushuser,0 adsi_arrivetimes,0 adsi_arriveuser,0 adsi_clicktimes,
0 adsi_clickuser,0 adsi_submituser,count(distinct username) adsi_registeruser 
from broadband.push_feedback where partdate='${DATE}' and from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd') ='${DATE}'
and feedbacktype=4 group by adid,url,from_unixtime(unix_timestamp(happentime,'yyyy-MM-dd HH:mm:ss'),'yyyyMMdd')
) temp where adsi_statday is not null and adsi_domain is not null
group by adid,adsi_domain,adsi_statday;" 1>>$LOGFILE 2>>$LOGFILE


sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table $TABLENAME --columns AD_ID,ADSI_STATDAY,ADSI_DOMAIN,ADSI_PUSHTIMES,ADSI_PUSHUSER,ADSI_ARRIVETIMES,ADSI_ARRIVEUSER,ADSI_CLICKTIMES,ADSI_CLICKUSER,ADSI_SUBMITUSER,ADSI_REGISTERUSER,ADSI_CREATETIME --export-dir ${OUTPUTPATH} --fields-terminated-by '\001' -m 2 --input-null-string '\\N'  --input-null-non-string '\\N'  1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

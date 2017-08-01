#!/bin/bash

#Created by liujq 2015-04-10
#premark tm_pm_enturltat

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tm_pm_enturltat
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db/$TABLENAME
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log


#rm desdata
hadoop dfs -rm -r $OUTPUTPATH/* 1>>$LOGFILE 2>>$LOGFILE


#mapreduce
hive -e "set mapred.job.name = premark_enturltat_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select cdomain,edomain,'3' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,citycode,$DATE
from aotain_dw.tw_fact_useracctag where partdate = '$DATE' and length(trim(cdomain)) > 0  and length(trim(edomain)) > 0 and length(trim(temtag)) > 0 
group by citycode,cdomain,edomain order by pv_cnt desc limit 100
union all 
select cdomain,edomain,'0' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,citycode,$DATE
from aotain_dw.tw_fact_useracctag where partdate = '$DATE' and length(trim(cdomain)) > 0  and length(trim(edomain)) > 0 and temtag = '0' 
group by citycode,cdomain,edomain order by pv_cnt desc limit 100
union all 
select cdomain,edomain,'1' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,citycode,$DATE
from aotain_dw.tw_fact_useracctag where partdate = '$DATE' and length(trim(cdomain)) > 0  and length(trim(edomain)) > 0 and temtag = '1' 
group by citycode,cdomain,edomain order by pv_cnt desc limit 100
union all 
select cdomain,edomain,'2' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,citycode,$DATE
from aotain_dw.tw_fact_useracctag where partdate = '$DATE' and length(trim(cdomain)) > 0  and length(trim(edomain)) > 0 and temtag = '2' 
group by citycode,cdomain,edomain order by pv_cnt desc limit 100;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

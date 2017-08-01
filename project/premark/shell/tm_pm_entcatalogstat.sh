#!/bin/bash

#Created by liujq 2015-04-13
#premark tm_pm_entcatalogstat

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tm_pm_entcatalogstat
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db/$TABLENAME
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log


#rm desdata
hadoop dfs -rm -r $OUTPUTPATH/* 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name = premark_entcatalogstat_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select curl.parent_id as cclassid,eurl.parent_id as eclassid,'3' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,tag.citycode,$DATE
from aotain_dw.tw_fact_useracctag tag join aotain_dim.to_url_class curl on tag.cdomain  = curl.host
join aotain_dim.to_url_class eurl on tag.edomain=eurl.host
where tag.partdate = '$DATE' and length(trim(tag.cdomain)) > 0  and length(trim(tag.edomain)) > 0 and length(trim(tag.temtag)) > 0 
group by tag.citycode,curl.parent_id,eurl.parent_id order by pv_cnt desc limit 100
union all
select curl.parent_id as cclassid,eurl.parent_id as eclassid,'0' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,tag.citycode,$DATE
from aotain_dw.tw_fact_useracctag tag join aotain_dim.to_url_class curl on tag.cdomain  = curl.host
join aotain_dim.to_url_class eurl on tag.edomain=eurl.host
where tag.partdate = '$DATE' and length(trim(tag.cdomain)) > 0  and length(trim(tag.edomain)) > 0 and temtag = '0' 
group by tag.citycode,curl.parent_id,eurl.parent_id order by pv_cnt desc limit 100
union all 
select curl.parent_id as cclassid,eurl.parent_id as eclassid,'1' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,tag.citycode,$DATE
from aotain_dw.tw_fact_useracctag tag join aotain_dim.to_url_class curl on tag.cdomain  = curl.host
join aotain_dim.to_url_class eurl on tag.edomain=eurl.host
where tag.partdate = '$DATE' and length(trim(tag.cdomain)) > 0  and length(trim(tag.edomain)) > 0 and temtag = '1' 
group by tag.citycode,curl.parent_id,eurl.parent_id order by pv_cnt desc limit 100
union all 
select curl.parent_id as cclassid,eurl.parent_id as eclassid,'2' as temtag,count(*) pv_cnt,count(distinct guid) uv_cnt,tag.citycode,$DATE
from aotain_dw.tw_fact_useracctag tag join aotain_dim.to_url_class curl on tag.cdomain  = curl.host
join aotain_dim.to_url_class eurl on tag.edomain=eurl.host
where tag.partdate = '$DATE' and length(trim(tag.cdomain)) > 0  and length(trim(tag.edomain)) > 0 and temtag = '2' 
group by tag.citycode,curl.parent_id,eurl.parent_id order by pv_cnt desc limit 100;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

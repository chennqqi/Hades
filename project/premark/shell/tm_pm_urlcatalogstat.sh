#!/bin/bash
#created by gqy 2015-04-14
#premark tm_pm_urlcatalogstat

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tm_pm_urlcatalogstat
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db/$TABLENAME
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log


#rm desdata
hadoop dfs -rm -r $OUTPUTPATH/* 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name=premark_$TABLENAME_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select url.parent_id,partdate,temtag,count(*) pv,count(distinct guid) uv from  aotain_dw.tw_fact_useracctag  tag
join aotain_dim.to_url_class url on  cast(tag.curltag as bigint) =url.class_id
where partdate='${DATE}' and length(trim(curltag)) > 0 group by parent_id,partdate,temtag
union all
select url.parent_id,partdate,'3' as temtag,count(*) pv,count(distinct guid) uv from  aotain_dw.tw_fact_useracctag  tag
join aotain_dim.to_url_class url on  cast(tag.curltag as bigint) =url.class_id
where partdate='${DATE}' and length(trim(curltag)) > 0 group by parent_id,partdate;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

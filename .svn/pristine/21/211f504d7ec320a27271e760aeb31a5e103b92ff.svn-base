#!/bin/bash
#created by gqy 2015-04-14
#premark tm_pm_urlclassstat

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tm_pm_urlclassstat
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db/$TABLENAME
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log


#rm desdata
hadoop dfs -rm -r $OUTPUTPATH/* 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name=premark_$TABLENAME_$DATE;
insert overwrite directory '${OUTPUTPATH}' 
select curltag,partdate, temtag,count(*) pv,count(distinct guid) uv from  aotain_dw.tw_fact_useracctag 
where partdate='${DATE}' and length(trim(curltag)) > 0 group by curltag,partdate,temtag
union all
select curltag,partdate,'3' as temtag,count(*) pv,count(distinct guid) uv from  aotain_dw.tw_fact_useracctag
where partdate='${DATE}' and length(trim(curltag)) > 0 group by curltag,partdate;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

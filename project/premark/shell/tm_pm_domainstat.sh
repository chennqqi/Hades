#!/bin/bash

#Created by wayne 2015-04-13
#premark tm_pm_domainstat


if [ $# -ne 2 ]; then
        exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tm_pm_domainstat
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db/$TABLENAME
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log

#rm data
hadoop fs -rm $OUTPUTPATH/*

#mapreduce
hive -e "set mapred.job.name = $TABLENAME;
insert overwrite directory '${OUTPUTPATH}' 
select cdomain,partdate,temtag,count(*) pv,count(distinct guid) uv from aotain_dw.tw_fact_useracctag where citycode = '$CITY' and partdate = '$DATE'
group by cdomain,partdate,temtag
union all
select cdomain,partdate,'-1' temtag,count(*) pv,count(distinct guid) uv from aotain_dw.tw_fact_useracctag where citycode = '$CITY' and partdate = '$DATE' group by cdomain,partdate;" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

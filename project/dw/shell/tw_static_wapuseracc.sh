#!/bin/bash

#Created by wayne 2014-12-14
#create a hour wap table for telecom report, http k-v data
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-5" ]; then
        DATE=`date -d "5 day ago" +%Y%m%d`
fi

TABLENAME=tw_static_wapuseracc
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE
BAKPATH=/user/hive/warehouse/aotain_bak

LOGFILE=/home/bigdata/project/dw/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hive -e "use aotain_dw;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name='$TABLENAME'_'$DATE';
insert overwrite table aotain_dw.$TABLENAME partition(citycode='$CITY',partdate='$DATE')
select a.msisdn,'Domain' attcode,a.spdomain attvalue,count(*),substring(timestamp,0,10)
from broadband.to_opr_wap a
where msisdn is not null and length(trim(msisdn))>0 and citycode='$CITY' and partdate='$DATE'
group by a.msisdn,a.spdomain,substring(timestamp,0,10)
union all
select a.msisdn,'URLNo' attcode,b.class_id attvalue,count(*),substring(timestamp,0,10)
from broadband.to_opr_wap a join aotain_dim.to_url_class b on a.spdomain = b.host
where msisdn is not null and length(trim(msisdn))>0 and citycode='$CITY' and partdate='$DATE'
group by a.msisdn,b.class_id,substring(timestamp,0,10);" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

DATE=`date -d "40 day ago" +%Y%m%d`
hive -e "use aotain_dw;alter table $TABLENAME drop partition(citycode='$CITY',partdate='$DATE');"
hadoop fs -rm -r /user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE

exit 0

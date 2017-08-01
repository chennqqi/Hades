#!/bin/bash

#create a hour areanetwork table for telecom report, http k-v data

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi


TABLENAME=tw_static_areauseracc
MRCLASS=com.aotain.dw.UserAttDriver
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE

LOGFILE=/home/bigdata/project/dw/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hive -e "use aotain_dw;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name='$TABLENAME'_'$DATE';
insert overwrite table aotain_dw.$TABLENAME partition(citycode='$CITY',partdate='$DATE')
select b.switchcode,'Domain' attcode,domain attvalue,count(*) frequency,substr(cast(accesstime as string),0,10) hour
from broadband.to_opr_http a,(select distinct switchcode from aotain_dim.to_ref_areanetwork
where citycode='$CITY' and partdate='$DATE') b
where a.citycode='$CITY' and a.partdate='$DATE' and a.username=b.switchcode
group by b.switchcode,domain,substr(cast(accesstime as string),0,10)
union all
select b.switchcode,'URLNo' attcode,urlclassid attvalue,count(*) frequency,substr(cast(accesstime as string),0,10) hour
from broadband.to_opr_http a,(select distinct switchcode from aotain_dim.to_ref_areanetwork
where citycode='$CITY' and partdate='$DATE') b
where a.citycode='$CITY' and a.partdate='$DATE' and a.username=b.switchcode and urlclassid > 0
group by b.switchcode,urlclassid,substr(cast(accesstime as string),0,10);" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

#rmdata
DATE=`date -d "40 day ago" +%Y%m%d`
hive -e "use aotain_dw;alter table $TABLENAME drop partition(citycode='$CITY',partdate='$DATE');"
hadoop fs -rm -r /user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE

exit 0

#!/bin/bash

#Created by wayne 2014-12-14
#create a hour table for telecom report, http k-v data
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tw_static_useracc
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE

LOGFILE=/home/bigdata/project/dw/log/"$TABLENAME"_"$CITY"_"$DATE".log

#mkdir
hive -e "use aotain_dw;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$CITY/$DATE';" 1>>$LOGFILE 2>>$LOGFILE

#mapreduce
hive -e "set mapred.job.name='$TABLENAME'_'$DATE';
insert overwrite table aotain_dw.$TABLENAME partition(citycode='$CITY',partdate='$DATE')
select username,'Domain' attcode,domain attvalue,count(*) frequency,substr(cast(accesstime as string),0,10) hour
from broadband.to_opr_http where citycode='$CITY' and partdate='$DATE'
group by username,domain,substr(cast(accesstime as string),0,10) 
union all
select username,'URLNo' attcode,urlclassid attvalue,count(*) frequency,substr(cast(accesstime as string),0,10) hour
from broadband.to_opr_http where citycode='$CITY' and partdate='$DATE' and urlclassid>0
group by username,urlclassid,substr(cast(accesstime as string),0,10);" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	exit $ret
fi

#rmdata
DATE=`date -d "40 day ago" +%Y%m%d`
hive -e "use aotain_dw;alter table $TABLENAME drop partition(citycode='$CITY',partdate='$DATE');"
hadoop fs -rm -r /user/hive/warehouse/aotain_dw.db/$TABLENAME/$CITY/$DATE 1>>$LOGFILE 2>>$LOGFILE

exit 0

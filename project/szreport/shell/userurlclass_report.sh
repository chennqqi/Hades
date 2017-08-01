#!/bin/bash

#Created by heyum 2015-03-31
#qqcount for report
#Version 2.0  modified by heyum 2015-03-31

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=userurlclass
OUTPUTPATH=/data02/$TABLENAME
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_"$CITY"_"$DATE".log

#rmdata
rm -f $OUTPUTPATH/*

#mapreduce
hive -e "insert overwrite local directory '$OUTPUTPATH'
row format delimited
fields terminated by ','
select username,citycode, class_name ,parent_name ,count(*) ,max(from_unixtime(accesstime,'yyyy-MM-dd HH:mm:ss'))
from broadband.to_opr_http t  join aotain_dim.to_ref_domainclass  u on t.urlclassid=u.class_id  where citycode='$CITY'and partdate='$DATE' 
and from_unixtime(accesstime,'yyyyMMdd HH:mm:ss')>='$DATE 20:00:00'
and from_unixtime(accesstime,'yyyyMMdd HH:mm:ss')<='$DATE 23:59:59'
group by citycode, username,parent_name ,class_name" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        rm -f $OUTPUTPATH/*
        exit $ret
fi
echo $DATE
#mergefile
cat $OUTPUTPATH/0* > $OUTPUTPATH/userurlclass"$DATE".csv
#sed -i "s/shenzhenshi/…Ó€⁄ –/g" `grep shenzhenshi -rl $OUTPUTPATH`
gzip $OUTPUTPATH/userurlclass"$DATE".csv 
rm -f $OUTPUTPATH/0*
DATE=`date -d "15 day ago" +%Y%m%d`
#ftpput
ftp -i -n 172.16.1.38 <<END 
user upfile  e%erVuFTc)V3
cd aotainftpsz/userurlclass
bin
lcd $OUTPUTPATH
delete userurlclass"$DATE".csv
put *

bye
#END
exit 0

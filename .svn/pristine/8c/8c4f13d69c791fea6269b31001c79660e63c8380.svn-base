#!/bin/bash

#Created by wayne 2014-12-14
#http keyword for report
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=sz_keywordacc_report
OUTPUTPATH=/data02/$TABLENAME
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_"$CITY"_"$DATE".log

#rmdata
rm -f $OUTPUTPATH/*

#mapreduce
hive -e "insert overwrite local directory '$OUTPUTPATH'
row format delimited
fields terminated by '|'
select 0,1,areaid,username,url,keyword,from_unixtime(accesstime,'yyyy-MM-dd HH:mm:ss')
from broadband.to_opr_http where citycode='$CITY' and partdate='$DATE'
and keyword is not null and length(trim(keyword))>0 and username like '%@%';" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        rm -f $OUTPUTPATH/*
        exit $ret
fi

#mergefile
cat $OUTPUTPATH/0* > $OUTPUTPATH/"$TABLENAME"_"$DATE".txt
gzip $OUTPUTPATH/"$TABLENAME"_"$DATE".txt $OUTPUTPATH
rm -f $OUTPUTPATH/0*

#ftpput
#ftp -i -n 219.134.184.76 <<END
#user aotian shenzhen755
ftp -i -n 121.15.207.189  <<END 
user ubasftp1 55774411
cd keyworddetail

lcd ${OUTPUTPATH}/
mdelete sz_keywordacc*
bin
put *
bye
ftp -i -n 219.134.184.76 <<END
user aotian shenzhen755
cd keywordetail
lcd ${OUTPUTPATH}/
bin
put *
bye
exit 0

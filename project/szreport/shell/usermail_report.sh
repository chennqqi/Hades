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

TABLENAME=usermail
OUTPUTPATH=/data02/$TABLENAME
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_"$CITY"_"$DATE".log

#rmdata
rm -f $OUTPUTPATH/*

#mapreduce
hive -e "insert overwrite local directory '$OUTPUTPATH'
row format delimited
fields terminated by ','
select userid,citycode,attvalue,frequency,lastdate from aotain_dw.tw_user_postinfo
where citycode='$CITY' and lastdate='$DATE'  and attcode='Mail'" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        rm -f $OUTPUTPATH/*
        exit $ret
fi
echo $DATE
#mergefile
cat $OUTPUTPATH/0* > $OUTPUTPATH/usermail"$DATE".csv
#sed -i "s/shenzhenshi/…Ó€⁄ –/g" `grep shenzhenshi -rl $OUTPUTPATH`
gzip $OUTPUTPATH/usermail"$DATE".csv 
rm -f $OUTPUTPATH/0*
DATE=`date -d "15 day ago" +%Y%m%d`
#ftpput
ftp -i -n 172.16.1.38 <<END 
user upfile  e%erVuFTc)V3
cd aotainftpsz/usermail
bin
lcd $OUTPUTPATH
delete usermail"$DATE".csv
put *

bye
#END
exit 0

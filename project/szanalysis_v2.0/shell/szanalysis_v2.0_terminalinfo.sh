#!/bin/bash

#Created by wayne 2014-12-14
#report terminal info for telecom
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=TerminalHttp_DailyReport
OUTPUTPATH=/data02/$TABLENAME
LOGFILE=/home/bigdata/project/szanalysis_v2.0/log/"$TABLENAME"_"$CITY"_"$DATE".log

#rmdata
rm -f $OUTPUTPATH/*

#mr
hive -e "insert overwrite local directory '$OUTPUTPATH'
row format delimited
fields terminated by '|'
select partdate,username,device,opersys,opersysver,count(*) cnt from broadband.to_opr_http 
where citycode='$CITY' and partdate='$DATE' and length(trim(device))>0 
group by partdate,username,device,opersys,opersysver" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	rm -f $OUTPUTPATH/*
        exit $ret
fi

#mergefile
#TerminalHttp_20150428.txt
REPORTFILE=TerminalHttp_"$DATE".txt

cat $OUTPUTPATH/0* > $OUTPUTPATH/"$REPORTFILE"
ret=$?
if [ $ret -ne 0 ]; then
        rm -f $OUTPUTPATH/*
        exit $ret
fi

#putfile
#scp $OUTPUTPATH/"$TABLENAME"_"$CITY"_"$DATE".txt root@172.16.1.38:/home/nebula/outdata/szanalysis_v2.0
#ret=$?
#if [ $ret -ne 0 ]; then
#        rm -f $OUTPUTPATH/*
#        exit $ret
#fi

mv -f $OUTPUTPATH/"$REPORTFILE" /home/nebula/outdata/szanalysis_v2.0/

exit 0

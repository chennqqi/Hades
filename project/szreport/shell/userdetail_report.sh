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

TABLENAME=userdetail
OUTPUTPATH=/data02/$TABLENAME
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_"$CITY"_"$DATE".log

#rmdata
#rm -f $OUTPUTPATH/*

#mapreduce
#hive -e " set mapred.reduce.tasks = 8; insert overwrite local directory '$OUTPUTPATH'
#row format delimited
#fields terminated by '||'
#select username,0,url,coalesce(urlclassid,0),from_unixtime(accesstime,'yyyy-MM-dd HH:mm:ss')
#from broadband.to_opr_http  where citycode='$CITY' and partdate='$DATE' and length(trim(url)) > 0
#and from_unixtime(accesstime,'yyyyMMdd HH:mm:ss')>='$DATE 20:00:00'
#and from_unixtime(accesstime,'yyyyMMdd HH:mm:ss')<='$DATE 21:00:00'
#"  1>>$LOGFILE 2>>$LOGFILE
#ret=$?
#if [ $ret -ne 0 ]; then
 #      rm -f $OUTPUTPATH/*
  #      exit $ret
#fi
echo $DATE  1>>$LOGFILE 2>>$LOGFILE
#mergefile
#cat $OUTPUTPATH/0* > $OUTPUTPATH/midcontent.txt

split -b 8G -d $OUTPUTPATH/midcontent.txt

#echo 'start gzip' 1>>$LOGFILE 2>>$LOGFILE
#rm -f  $OUTPUTPATH/0*
#countFile=`ls -l |grep "^-"|wc -l | awk '{print $1}'`
#echo $countFile 1>>$LOGFILE 2>>$LOGFILE
#for((i=0;i<=countFile;i++));
#do
#    gzip  $OUTPUTPATH/x0$i
#     mv   $OUTPUTPATH/x0$i.gz   $OUTPUTPATH/userdetail"$DATE"_$i.txt.gz
#done;
#echo 'split file done' 1>>$LOGFILE 2>>$LOGFILE
#gzip $OUTPUTPATH/userdetail"$DATE".txt
#rm -f $OUTPUTPATH/midcontent.txt
echo 'statr ftp' 1>>$LOGFILE 2>>$LOGFILE
#ftpput
#ftp -i -n 121.15.207.189  <<END 
#user ubasftp1 55774411
#cd  userdetail
#bin
#lcd $OUTPUTPATH
#mdelete *
#put *
#bye
#END
echo 'end ftp' 1>>$LOGFILE 2>>$LOGFILE
exit 0

#!/bin/bash

#Created by wayne 2014-12-14
#http keyword for report
#Version 2.0  modified by wayne 2014-12-14
if [ $# -ne 1 ]; then
        exit 1
fi

DATE=$1
if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi
TABLENAME=sz_qqcount_report
INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/shenzhen/$DATE
OUTPUTPATH=/data02/$TABLENAME
HDFSOUTPUTAPTH=/user/project/output/$TABLENAME/
MRCLASS=com.aotain.hdfs.mr.CountqqDriver
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_$DATE.log
rm -rf $OUTPUTPATH/*
hadoop jar /home/bsmp/work/mr/countqq.jar $MRCLASS   $INPUTPATH $HDFSOUTPUTAPTH 1234 $DATE 1>>$LOGFILE 2>>$LOGFILE 
ret=$?
if [ $ret -ne 0 ]; then
	hadoop fs -rm -r $HDFSOUTPUTAPTH
        exit $ret
fi


hadoop fs -get  $HDFSOUTPUTAPTH/part*  ${OUTPUTPATH}/
hadoop fs -rm -r $HDFSOUTPUTAPTH

#mergefile
cat $OUTPUTPATH/part* > $OUTPUTPATH/userqqnumber"$DATE".csv 
gzip $OUTPUTPATH/userqqnumber"$DATE".csv  

rm -f $OUTPUTPATH/part*



#ftpput
ftp -i -n 219.134.184.76 <<END
user aotian shenzhen755
cd qqdetail
lcd ${OUTPUTPATH}/
bin
put *
bye
exit 0

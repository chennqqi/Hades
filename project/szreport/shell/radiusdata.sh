#!/bin/bash

#Created by gqy  2015-04-1

if [ $# -ne 1 ]; then
        exit 1
fi

DATE=$1
if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi
TABLENAME=to_opr_radiusorig
INPUTPATH=/user/hive/warehouse/broadband.db/radiusorig/guangdong/$DATE
OUTPUTPATH=/data02/radiusorig
CITYCONFIG=/home/bigdata/project/szreport/config/prefix.conf
HDFSOUTPUTAPTH=/user/project/output/radiusorig/$DATE
MRCLASS=com.aotain.project.szreport.RadiusDataDriver
LOGFILE=/home/bigdata/project/szreport/log/"$TABLENAME"_$DATE.log
MRPATH=/home/bigdata/project/szreport/mr

DEL_PARTITION_DATE=`date -d "9 day ago" +%Y%m%d`


rm -rf $OUTPUTPATH/*

hadoop jar $MRPATH/szreport.jar $MRCLASS $INPUTPATH $HDFSOUTPUTAPTH $CITYCONFIG 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
	hadoop fs -rm -r $HDFSOUTPUTAPTH
        exit $ret
fi


hadoop fs -get  $HDFSOUTPUTAPTH/part*  ${OUTPUTPATH}/
hadoop fs -rm -r $HDFSOUTPUTAPTH

#mergefile
cat $OUTPUTPATH/part* > $OUTPUTPATH/"radiusorig$DATE".csv 
gzip $OUTPUTPATH/"radiusorig$DATE".csv  $OUTPUTPATH 

rm -f $OUTPUTPATH/part*
echo "delete ${OUTPUTPATH} data" >> $LOGFILE

hive -e "use broadband;alter table $TABLENAME drop partition (partdate=$DEL_PARTITION_DATE)" 
hadoop fs -rm -r /user/hive/warehouse/broadband.db/radiusorig/guangdong/$DEL_PARTITION_DATE 

echo 'delete table to_opr_radiusorig data in date ${DEL_PARTITION_DATE}' >> $LOGFILE
echo "begin to upload......" >>$LOGFILE

#ftp
ftp -i -n 219.134.184.76 <<END
user aotian shenzhen755
cd radiusorig
lcd ${OUTPUTPATH}/
bin
put *
bye
exit 0

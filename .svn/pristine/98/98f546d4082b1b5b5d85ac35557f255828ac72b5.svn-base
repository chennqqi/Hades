#!/bin/bash

#creator by gqy
#creator time 20150428
#function 

DATE=$1
CITY=$2

if [ $# -ne 2 ];then
        echo "args is not 2"
        exit 1
fi

if [ $1 = "-1" ];then
         DATE=`date -d "1 day ago" +%Y%m%d`
fi

TABLENAME=tmp_http
MRCLASS=com.aotain.project.other.TmpHttpDriver
INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/$CITY/$DATE
OUTPUTPATH=/user/hive/warehouse/temp.db/tmp_http/$CITY/$DATE
LOGFILE=/home/bigdata/project/other/log/${TABLENAME}_${CITY}_${DATE}.log
MRPATH=/home/bigdata/project/other/mr

echo "DATE:$DATE" | tee -a $LOGFILE
echo "CITY:$CITY " | tee -a $LOGFILE
echo "MRCLASS:$MRCLASS " | tee -a $LOGFILE
echo "TABLENAME:$TABLENAME " | tee -a $LOGFILE
echo "INPUTPATH:$INPUTPATH " | tee -a $LOGFILE
echo "OUTPUTPATH:$OUTPUTPATH " | tee -a $LOGFILE

hadoop jar $MRPATH/other.jar $MRCLASS $INPUTPATH $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ];then
	echo "hadoop jar is fail" | tee -a $LOGFILE
	exit $ret
fi
echo "hadoop jar is success" | tee -a $LOGFILE

hive -e "use temp;alter table $TABLENAME add partition(citycode='$CITY',partdate='$DATE') location '$OUTPUTPATH'" 1>>$LOGFILE 2>>$LOGFILE

echo "excute all!!!" | tee -a $LOGFILE
exit 0


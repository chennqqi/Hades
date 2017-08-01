#!/bin/bash


#Created by turk 2015-01-16
#Version 1.1 add cityname
#Version 1.2 modified by turk 

if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/ecommerce
JAVALIB=${WORKPATH}/mr/
LOGFILE=${WORKPATH}/log/post_format_output_${DATE}_${CITYNAME}.log
MRJAR=$JAVALIB/ecommerce.jar

echo `date +"%Y-%m-%d %H:%M:%S"`  "------Post Format------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.2 update(2015-02-04)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


CONFPATH=${WORKPATH}/config
LOCALOUTPUTPATH=/data02/post

#HDFS--
DATAPATH=/user/hive/warehouse/broadband.db/post/${CITYNAME}/${DATE}
OUTPUTPATH=/user/project/output/postformat/${CITYNAME}_${DATE}
#HDFS--end 

#hdfs mapreduce
echo `date +"%Y-%m-%d %H:%M:%S"`      "Map reduce Start....[$DATE]" | tee -a $LOGFILE
echo "DATA PATH:" $DATAPATH
echo "OUTPUT PATH:" $OUTPUTPATH

hadoop jar ${MRJAR} com.aotain.dw.PostFormatOutputDriver $DATAPATH $OUTPUTPATH $CONFPATH/postformat.xml 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

mkdir -p ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}
rm -f ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/*
echo `date +"%Y-%m-%d %H:%M:%S"`      "Get file to local path" | tee -a $LOGFILE
hadoop dfs -get $OUTPUTPATH/part* ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE} 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

cat ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/* >> ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/post_${CITYNAME}_${DATE}.txt
hadoop dfs -rm -r $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE

#delete history file
OLDDATE=`date -d "10 day ago"   +%Y%m%d`
rm -rf ${LOCALOUTPUTPATH}/${CITYNAME}/${OLDDATE}

#upload ftp
ftp -i -n <<FTPIT
open 172.16.1.38
user upfile e%erVuFTc)V3
bin
passive
put ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/post_${CITYNAME}_$DATE.txt /aotainftp/post/post_${CITYNAME}_$DATE.txt
quit                                                     
FTPIT
#----------------ftp end-------------------------

echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


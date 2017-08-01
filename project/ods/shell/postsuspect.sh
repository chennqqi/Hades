#!/bin/bash


#Created by turk 2015-03-05
#Version 1.0 

exit 0;

if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago"   +%Y%m%d`
fi

WORKPATH=/home/bsmp/work/
JAVALIB=/home/bsmp/work/mr/
LOGFILE=${WORKPATH}/log/postsuspect_${DATE}_${CITYNAME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------Post Suspect------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-03-05)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


#HDFS--
DATAPATH=/user/hive/warehouse/broadband.db/postsuspect/${CITYNAME}/${DATE}
OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/tw_usertel/${CITYNAME}/${DATE}
MOBILEAREAPATH=/user/hive/warehouse/aotain_dim.db/to_ref_mobile_area
MAILPATH=/user/hive/warehouse/aotain_dw.db/tw_usermail/${CITYNAME}/${DATE}
#HDFS--end


#hdfs mapreduce
echo `date +"%Y-%m-%d %H:%M:%S"`      "Map reduce Start....[$DATE]" | tee -a $LOGFILE
echo "DATA PATH:" $DATAPATH
echo "MOBILEAREAPATH PATH:" $MOBILEAREAPATH
echo "OUTPUT PATH:" $OUTPUTPATH

hadoop jar $JAVALIB/PostSuspect.jar ${DATAPATH} ${MOBILEAREAPATH} ${OUTPUTPATH} ${CITYNAME}_${DATE} 
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

hadoop fs -rm -r ${MAILPATH}/*
hadoop fs -mv ${OUTPUTPATH}/*MAIL* ${MAILPATH}

#mkdir -p ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}
#rm -f ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/*
#echo `date +"%Y-%m-%d %H:%M:%S"`      "Get file to local path" | tee -a $LOGFILE
#hadoop dfs -get $OUTPUTPATH/part* ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE} 1>>$LOGFILE 2>>$LOGFILE
#ret=$?
#if [ ${ret} -ne 0 ]; then
#        exit ${ret}
#fi

#cat ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/* >> ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/post_${CITYNAME}_${DATE}.txt
#hadoop dfs -rm -r $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE

#delete history file
#OLDDATE=`date -d "10 day ago"   +%Y%m%d`
#rm -rf ${LOCALOUTPUTPATH}/${CITYNAME}/${OLDDATE}

echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0

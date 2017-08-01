#!/bin/bash

#Created by turk 2015-04-10
#Project  Terminal
#Version 1.0
#Target Tatable tm_pm_guidurltag
#

DATE=$1
WORKPATH=/home/bigdata/project/premark/
LOGFILE=$WORKPATH/log/tm_pm_guidcookiemap.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Portal           --------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.1 update(2015-04-10)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE



export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar
#ORACLE
IP=172.16.1.35
SERVICENAME=BSMP
USERNAME=infi
PASSWD=infi2015
#HDFS
OUTPUTPATH=/user/hive/warehouse/aotain_dm_premark.db


hive -e "insert overwrite directory '${OUTPUTPATH}/tm_pm_guidcookiemap' select guid,cdomain,cookie,max(partdate) as partdate from aotain_dw.tw_fact_useracctag where coalesce(cookie,'') != ''  group by guid,cdomain,cookie "  1>>$LOGFILE 2>>$LOGFILE


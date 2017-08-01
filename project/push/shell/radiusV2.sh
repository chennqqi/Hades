#!/bin/bash

#Created by cheng 2015-06-15
#Version 1.0 

if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago"   +%Y%m%d`
fi

WORKPATH=/home/bigdata/project/push
JAVALIB=/home/bigdata/project/push/mr
LOGFILE=${WORKPATH}/log/HBase_HTTP_PUSH_RADIUS_${CITYNAME}_${DATE}.log
CONFIGPATH=${WORKPATH}/config/

echo `date +"%Y-%m-%d %H:%M:%S"`  "------PUSH_RADIUS------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-06-15)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

#HDFS--
DATAPATH=/user/hive/warehouse/broadband.db/ipsradius/${CITYNAME}/${DATE}
#OUTPUTPATH=/user/hive/warehouse/aotain_dw.db/tw_userphone/${CITYNAME}/${DATE}
#HDFS--end


export HBASELIB=/opt/cloudera/parcels/CDH/lib/hbase
export HADOOP_CLASSPATH=$HBASELIB/hbase-client.jar:$HBASELIB/hbase-common.jar:$HBASELIB/hbase-hadoop2-compat.jar:$HBASELIB/hbase-hadoop-compat.jar:$HBASELIB/hbase-it.jar:$HBASELIB/hbase-prefix-tree.jar:$HBASELIB/hbase-protocol.jar:$HBASELIB/hbase-server.jar:$HBASELIB/hbase-shell-0.96.1.1-cdh5.0.3.jar:$HBASELIB/hbase-thrift-0.96.1.1-cdh5.0.3.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/high-scale-lib-1.1.1.jar

#hadoop jar $JAVALIB/radiusV2.jar com.aotain.push.RadiusIntoHbase ${DATAPATH} ${CITYNAME}_${DATE} 
hadoop jar $JAVALIB/radiusV2.jar com.aotain.push.RadiusIntoHbase ${DATAPATH} ${CITYNAME}_${DATE} 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0

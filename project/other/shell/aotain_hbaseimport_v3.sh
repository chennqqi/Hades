#!/bin/bash

#Created by turk 2014-12-04
#Version 1.1  modified by turk 2014-12-17 FOR partition 
#Version 1.1  modified by turk 2014-12-30 FOR fix bug
#Version 3.0  modified by turk 2015-01-29 FOR source data from httpdc
#Version 3.0	modified by turk 2015-02-09 FOR single file
#Version 3.1	modified by turk 2015-02-26 FOR add 10 file commit

if [ $# -lt 3 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><TABLENAME><CITYNAME>"
	exit 1
fi

DATE=$1
TABLENAME=$2
CITYNAME=$3

WORKPATH=/root/work/
JAVALIB=/root/work/mr/
LOGFILE=$WORKPATH/log/HBase_HTTP_${TABLENAME}_${CITYNAME}_${DATE}.log
CONFIGPATH=/root/work/config/

echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 3.1 update(2015-02-26)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Include create partition"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "--------------------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

PARTITION=${DATE:0:6}
TABLENAME=${TABLENAME}_${CITYNAME}

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:PARTITION/DATE:${DATE} INPUTPATH:${INPUTPATH} TABLENAME:${TABLENAME}" | tee -a $LOGFILE

#HDFS--


INPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/${CITYNAME}/
DATAPATH=${INPUTPATH}/${DATE}
OUTPUTPATH=/user/project/output/${TABLENAME}/${CITYNAME}_${DATE}
#HDFS--end
	
	
#hdfs mapreduce 
echo `date +"%Y-%m-%d %H:%M:%S"`      "Start....[$DATE]" | tee -a $LOGFILE
echo "DATA PATH:" $DATAPATH
echo "OUTPUT PATH:" $OUTPUTPATH


#SET CLASSPATH
export HBASELIB=/opt/cloudera/parcels/CDH/lib/hbase
export HADOOP_CLASSPATH=$HBASELIB/hbase-client.jar:$HBASELIB/hbase-common.jar:$HBASELIB/hbase-hadoop2-compat.jar:$HBASELIB/hbase-hadoop-compat.jar:$HBASELIB/hbase-it.jar:$HBASELIB/hbase-prefix-tree.jar:$HBASELIB/hbase-protocol.jar:$HBASELIB/hbase-server.jar:$HBASELIB/hbase-shell-0.96.1.1-cdh5.0.3.jar:$HBASELIB/hbase-thrift-0.96.1.1-cdh5.0.3.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core.jar

hadoop dfs -ls ${DATAPATH}/*.lzo | awk '{print $8}' > ${CONFIGPATH}/hdfsfile_${CITYNAME}_${DATE}
TABLENAME=${TABLENAME}_${PARTITION}
FILENUM=`cat ${CONFIGPATH}/hdfsfile_${CITYNAME}_${DATE} | wc -l`

COUNT=0
BUFFPATH=/user/project/input/http/${CITYNAME}
hadoop dfs -rm -r ${BUFFPATH}
hadoop dfs -mkdir ${BUFFPATH}

function fHFileImport()
{
  echo `date +"%Y-%m-%d %H:%M:%S"`       "START MR..." | tee -a $LOGFILE
	hadoop jar $JAVALIB/Hades-1.1.jar com.aotain.hbase.dataimport.HFileOutput ${BUFFPATH} ${OUTPUTPATH} tablename=${TABLENAME} config=${CONFIGPATH}/httpget.xml partition=${PARTITION} time=${DATE} 1>>$LOGFILE 2>>$LOGFILE
	ret=$?
	if [ ${ret} -ne 0 ]; then
		exit ${ret}
	fi
	#HFILE
	echo `date +"%Y-%m-%d %H:%M:%S"`      "HFile ${HDFSFILE}  Success! Start load data" | tee -a $LOGFILE
	
	#Load
	#hadoop jar $HBASELIB/hbase-server.jar completebulkload ${OUTPUTPATH} ${TABLENAME} -Dhbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily=128 1>>$LOGFILE 2>>$LOGFILE
	#ret=$?
	#if [ ${ret} -ne 0 ]; then
	#	exit ${ret}
	#fi
								
	#DELETE OUTPUTFILE 
	hadoop dfs -rm -r ${OUTPUTPATH}
	BUFFPATH=/user/project/input/http/${CITYNAME}
	hadoop dfs -rm -r ${BUFFPATH}
	hadoop dfs -mkdir ${BUFFPATH}  
}

 if [ $FILENUM -ge 1 ]; then
	        for num in $( seq 1 $FILENUM )
	        do
	        	HDFSFILE=`cat ${CONFIGPATH}/hdfsfile_${CITYNAME}_${DATE} | sed -n "$num"p`
	        	echo `date +"%Y-%m-%d %H:%M:%S"`       "HDFSFILE:${HDFSFILE}" | tee -a $LOGFILE
	        	if [[ ${HDFSFILE} =~ ".lzo" ]]; then
	        		echo `date +"%Y-%m-%d %H:%M:%S"`       "COPY FILE:${HDFSFILE}" | tee -a $LOGFILE
	          	
		        	
		        	hadoop dfs -cp ${HDFSFILE} ${BUFFPATH}
		        	hadoop dfs -cp ${HDFSFILE}.index ${BUFFPATH}
		        	COUNT=$[COUNT+1]
		        	
		        	if [ $COUNT = 5 ]; then
		        		fHFileImport
		        		COUNT=0
							fi
	          fi
	        done
	        fHFileImport
 fi	        
 
rm -f ${CONFIGPATH}/hdfsfile_${CITYNAME}_${DATE}

echo `date +"%Y-%m-%d %H:%M:%S"`      "Load data Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0

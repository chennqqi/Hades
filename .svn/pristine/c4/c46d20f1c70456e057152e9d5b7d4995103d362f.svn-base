#!/bin/bash

#Created by turk 2015-08-06
#Data Merger FOR CU
#Version 1.0



if [ $# -lt 3 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<IDC> <DATE> <CITYNAME>"
	exit 1
fi

IDC=$1
DATE=$2
HOUR=$3


WORKPATH=/home/bsmp/work
JAVALIB=${WORKPATH}/lib/
LOGFILE=$WORKPATH/log/DataMerger_${IDC}_"$DATE"_"$HOUR".log
MRJAR=${JAVALIB}/idcspark.jar


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-08-06)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Data Merger"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE"   | tee -a $LOGFILE

#HDFS--
INPUTPATH=hdfs://nameservice1/user/hive/warehouse/idc_access_log/${IDC}/${DATE}/${HOUR}
OUTPUTPATH=hdfs://nameservice1/user/hive/warehouse/sds.db/sds_access_log/${IDC}/${DATE}/${HOUR}
#HDFS--end

#hdfs mapreduce
echo "DATA PATH:" $INPUTPATH | tee -a $LOGFILE
echo "OUTPUT PATH:" $OUTPUTPATH | tee -a $LOGFILE

#spark
spark-submit --class com.aotain.ods.IDCDataMerger --deploy-mode cluster --master yarn --num-executors 1200 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} $INPUTPATH/*.lzo $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi
echo `date +"%Y-%m-%d %H:%M:%S"`      "Data Cleaning Success" | tee -a $LOGFILE


########lzo index############################
#hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer ${OUTPUTPATH} 1>>$LOGFILE 2>>$LOGFILE
#ret=$?
#if [ ${ret} -ne 0 ]; then
#        exit ${ret}
#fi


hadoop fs -ls ${OUTPUTPATH}/*.lzo | awk {'print $8'} > ${WORKPATH}/config/hdfsfile_${IDC}_${DATA}_${HOUR}.txt
FILENUM=`cat ${WORKPATH}/config/hdfsfile_${IDC}_${DATA}_${HOUR}.txt | wc -l`
COUNT=0
if [ $FILENUM -ge 1 ]; then
	        for num in $( seq 1 $FILENUM )
	        do
	        	HDFSFILE=`cat ${WORKPATH}/config/hdfsfile_${IDC}_${DATA}_${HOUR}.txt | sed -n "$num"p`
	        	echo `date +"%Y-%m-%d %H:%M:%S"`       "HDFSFILE:${HDFSFILE}" | tee -a $LOGFILE
	        	#if [[ ${HDFSFILE} =~ ".lzo" ]]; then
	        		echo `date +"%Y-%m-%d %H:%M:%S"`       "CREATE INDEX FILE:${HDFSFILE}" | tee -a $LOGFILE
	          		{
	          		 	hadoop jar /opt/cloudera/parcels/GPLEXTRAS/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer ${HDFSFILE} 1>>$LOGFILE 2>>$LOGFILE
					} &
							
		        	COUNT=$[COUNT+1]
		        	sleep 3
	          	#fi
	          if [ $COUNT = 8 ]; then
		        wait
				COUNT=0
			  fi
	        done
fi
rm -f ${WORKPATH}/config/hdfsfile_${IDC}_${DATA}_${HOUR}.txt
echo `date +"%Y-%m-%d %H:%M:%S"`      "Created Index Success" | tee -a $LOGFILE

exit 0


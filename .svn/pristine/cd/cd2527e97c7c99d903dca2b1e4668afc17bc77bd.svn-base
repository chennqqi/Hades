#!/bin/bash

#Created by turk 2015-01-20
#Http Data Cleaning For broadband speed
#Version 1.0  
#Version 1.1 Add param city
#Version 1.2 Record invalid data



if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/ods
JAVALIB=${WORKPATH}/mr/
LOGFILE=$WORKPATH/log/HTTP_DC_"$CITYNAME"_"$DATE".log
MRJAR=${JAVALIB}/ods.jar


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.2 (2015-02-04)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Http Data Cleaning For broadband speed"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE"   | tee -a $LOGFILE

#HDFS--
INPUTPATH=hdfs://nameservice1/user/hive/warehouse/broadband.db/http/"$CITYNAME"
OUTPUTPATH=hdfs://nameservice1/user/hive/warehouse/broadband.db/to_opr_http/"$CITYNAME"
CONFIGFILE=hdfs://nameservice1/user/project/conf/blacklist.txt

DATAPATH="$INPUTPATH"/"$DATE"
OUTPUTPATH="$OUTPUTPATH"/"$DATE"
#HDFS--end

#hdfs mapreduce
echo "DATA PATH:" $DATAPATH | tee -a $LOGFILE
echo "OUTPUT PATH:" $OUTPUTPATH | tee -a $LOGFILE

REMARK=${CITYNAME}_${DATE}

#mapreduce
#hadoop jar ${MRJAR} com.aotain.ods.HTTPFilterDriver $DATAPATH $OUTPUTPATH $CONFIGFILE $REMARK 1>>$LOGFILE 2>>$LOGFILE
#ret=$?
#if [ ${ret} -ne 0 ]; then
#        exit ${ret}
#fi
#echo `date +"%Y-%m-%d %H:%M:%S"`      "Data Cleaning Success" | tee -a $LOGFILE

#spark
spark-submit --class com.aotain.ods.HTTPDCSpark --deploy-mode cluster --master yarn --num-executors 700 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} $DATAPATH/*.lzo $OUTPUTPATH ${DATE} 1>>$LOGFILE 2>>$LOGFILE
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


hadoop fs -ls ${OUTPUTPATH}/*.lzo | awk {'print $8'} > ${WORKPATH}/config/hdfsfile_${CITYNAME}.txt
FILENUM=`cat ${WORKPATH}/config/hdfsfile_${CITYNAME}.txt | wc -l`
COUNT=0
if [ $FILENUM -ge 1 ]; then
	        for num in $( seq 1 $FILENUM )
	        do
	        	HDFSFILE=`cat ${WORKPATH}/config/hdfsfile_${CITYNAME}.txt | sed -n "$num"p`
	        	echo `date +"%Y-%m-%d %H:%M:%S"`       "HDFSFILE:${HDFSFILE}" | tee -a $LOGFILE
	        	#if [[ ${HDFSFILE} =~ ".lzo" ]]; then
	        		echo `date +"%Y-%m-%d %H:%M:%S"`       "CREATE INDEX FILE:${HDFSFILE}" | tee -a $LOGFILE
	          		{
	          		 	hadoop jar /opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/hadoop-lzo.jar com.hadoop.compression.lzo.LzoIndexer ${HDFSFILE} 1>>$LOGFILE 2>>$LOGFILE
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
rm -f ${WORKPATH}/config/hdfsfile_${CITYNAME}.txt






#delete hdfs history file
OLDDATE=`date -d "8 day ago"   +%Y%m%d`
hadoop dfs -rm -r ${INPUTPATH}/${OLDDATE} 1>>$LOGFILE 2>>$LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Delete Hdfs File Success!" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


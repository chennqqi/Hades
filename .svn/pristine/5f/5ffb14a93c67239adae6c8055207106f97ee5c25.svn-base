#!/bin/bash

#Created by turk 2014-11-21
#Version 1.2


#Created by turk 2014-12-04
#hbase import for day->hour
#Version 1.1  modified by turk 2014-12-30 new source data
#Version 1.2  modified by turk 2014-12-30 new file path

echo `date +"%Y-%m-%d %H:%M:%S"`  "------Ping An URL------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.2 update(2015-01-29)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


DATE=$1

if [ "$DATE" = "" ]; then
	DATE=`date -d "1 day ago"   +%Y%m%d`
fi

WORKPATH=/home/bigdata/project/cnshuidi
JAVALIB=${WORKPATH}/mr/
LOGFILE=$WORKPATH/log/pinganurl_$DATE.log
MRJAR=$JAVALIB/cnshuidi.jar

#HDFS--
DATAPATH=/user/hive/warehouse/broadband.db/to_opr_http/shenzhen/$DATE
OUTPUTPATH=/user/project/input/pinganurl/$DATE
CONFPATH=/user/project/conf
#HDFS--end

LOCALOUTPUTPATH=/data02/dem/

#hdfs mapreduce

echo `date +"%Y-%m-%d %H:%M:%S"`      "Start....[$DATE]" | tee -a $LOGFILE
echo "DATA PATH:" $DATAPATH
echo "OUTPUT PATH:" $OUTPUTPATH

hadoop dfs -mkdir $OUTPUTPATH/
hadoop dfs -mkdir $OUTPUTPATH/nocar_edm/
hadoop dfs -mkdir $OUTPUTPATH/gf_edm/

hadoop jar $MRJAR com.aotain.hdfs.mr.PingAnDriver $DATAPATH $OUTPUTPATH/nocar_edm/ $CONFPATH/nocar_edm.txt 1>>$LOGFILE 2>>$LOGFILE
hadoop jar $MRJAR com.aotain.hdfs.mr.PingAnDriver $DATAPATH $OUTPUTPATH/gf_edm/ $CONFPATH/gf_edm.txt 1>>$LOGFILE 2>>$LOGFILE

hadoop dfs -get $OUTPUTPATH/nocar_edm/part-r-00000 $LOCALOUTPUTPATH/sz_bank_nocar_dem_$DATE.txt 1>>$LOGFILE 2>>$LOGFILE
hadoop dfs -get $OUTPUTPATH/gf_edm/part-r-00000 $LOCALOUTPUTPATH/sz_bank_$DATE.txt 1>>$LOGFILE 2>>$LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`      "Start GZIP....[$DATE]" | tee -a $LOGFILE
rm -f $LOCALOUTPUTPATH/sz_bank_nocar_dem_"$DATE".txt.gz
rm -f $LOCALOUTPUTPATH/sz_bank_"$DATE".txt.gz

gzip $LOCALOUTPUTPATH/sz_bank_nocar_dem_"$DATE".txt
gzip $LOCALOUTPUTPATH/sz_bank_"$DATE".txt

#delete history file
OLDDATE=`date -d "10 day ago"   +%Y%m%d`
rm -f $LOCALOUTPUTPATH/sz_bank_nocar_dem_"$OLDDATE".txt.gz
rm -f $LOCALOUTPUTPATH/sz_bank_"$OLDDATE".txt.gz

echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE


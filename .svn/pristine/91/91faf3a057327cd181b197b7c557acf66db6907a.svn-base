#!/bin/bash

#Created by turk 2015-07-10
#Terminal Data Join
#Version 1.0  


if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/terminal
JAVALIB=${WORKPATH}/mr/
LOGFILE=$WORKPATH/log/Terminal_Data_Join_${CITYNAME}_"$DATE".log
MRJAR=${JAVALIB}/terminal.jar


echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 (2015-07-10)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Terminal Data Join"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE INPUTPATH:$INPUTPATH OUTPUTPATH:$OUTPUTPATH"   | tee -a $LOGFILE

#HDFS--
INPUTPATHDAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_mbanalysis_day/${CITYNAME}/${DATE}
OUTPUTPATHDAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_terminal_info/${CITYNAME}/${DATE}

#--7DAY
INPUTPATH7DAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_mbanalysis_7day/${CITYNAME}/${DATE}
OUTPUTPATH7DAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_terminal_info_7day/${CITYNAME}/${DATE}

#--15DAY
INPUTPATH15DAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_mbanalysis_15day/${CITYNAME}/${DATE}
OUTPUTPATH15DAY=/user/hive/warehouse/aotain_dm_terminalstat.db/tm_terminal_info_15day/${CITYNAME}/${DATE}

#HDFS--end

#hdfs mapreduce
echo "DATA PATH:" $DATAPATH | tee -a $LOGFILE
echo "OUTPUT PATH:" $OUTPUTPATH | tee -a $LOGFILE

#mapreduce
#hadoop jar $JAVALIB/Hades-1.1.jar com.aotain.spark.TerminalDC $DATAPATH $OUTPUTPATH 1>>$LOGFILE 2>>$LOGFILE
#if [ $? -ne 0 ]; then
#        exit $?
#fi

#spark-day
spark-submit --class com.aotain.project.terminal.TerminalDataJoin --deploy-mode cluster --master yarn --num-executors 480 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} ${INPUTPATHDAY} ${OUTPUTPATHDAY} 1>>$LOGFILE 2>>$LOGFILE
if [ $? -ne 0 ]; then
        exit $?
fi

#spark-7day
spark-submit --class com.aotain.project.terminal.TerminalDataJoin --deploy-mode cluster --master yarn --num-executors 480 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} ${INPUTPATH7DAY} ${OUTPUTPATH7DAY} 1>>$LOGFILE 2>>$LOGFILE
if [ $? -ne 0 ]; then
        exit $?
fi

#spark-15day
spark-submit --class com.aotain.project.terminal.TerminalDataJoin --deploy-mode cluster --master yarn --num-executors 480 --driver-memory 4g --executor-memory 2g --executor-cores 1 --queue thequeue ${MRJAR} ${INPUTPATH15DAY} ${OUTPUTPATH15DAY} 1>>$LOGFILE 2>>$LOGFILE
if [ $? -ne 0 ]; then
        exit $?
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Success!" | tee -a $LOGFILE

echo `date +"%Y-%m-%d %H:%M:%S"`      "Data Join Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


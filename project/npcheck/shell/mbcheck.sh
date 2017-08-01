#!/bin/bash
if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2
TABLENAME=tm_mbcheck
MRCLASS=com.aotain.project.npcheck.MBCheckDriver
#INPUTPATH=/user/data/test/testnpin
if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

OUTPUTPATH=/user/hive/warehouse/aotain_dm_npcheck.db/tm_mbcheck/$DATE
CONFIG=/home/bigdata/project/npcheck/config/hfile-NPointCheck.xml
#WORKOUTPUTPATH=/user/project/output/mbcheck/
WORKINPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/$CITY/$DATE
MRPATH=/home/bigdata/project/npcheck/mr
LOGFILE=/home/bigdata/project/npcheck/log/"$TABLENAME"_"$CITY"_"$DATE".log

echo "DATE:$DATE"|tee -a $LOGFILE
echo "CITY:$CITY" | tee -a $LOGFILE
echo "OUTPUTPATH:$OUTPUTPATH" | tee -a $LOGFILE
echo "CONFIG:$CONFIG" | tee -a $LOGFILE
echo "WORKINPUTPATH:$WORKINPUTPATH" | tee -a $LOGFILE


#mapreduce
hive -e "use aotain_dm_npcheck;alter table $TABLENAME add partition(partdate='$DATE') location '$DATE';" 1>>$LOGFILE 2>>$LOGFILE
hadoop jar $MRPATH/npcheck.jar $MRCLASS $WORKINPUTPATH $OUTPUTPATH $CONFIG $DATE 1>>$LOGFILE 2>>$LOGFILE 
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

SDAY=`date -d "$1 day ago"  +%Y%m%d`
echo $SDAY
DAY=$(($1+6))
EDAY=`date -d "$DAY day ago"  +%Y%m%d`
echo $EDAY



#rm data
RMDATE=`date -d "9 day ago" +%Y%m%d`
hive -e "use aotain_dm_npcheck;alter table $TABLENAME  drop partition(partdate='$RMDATE');"
#hadoop fs -rm -r /user/hive/warehouse/aotain_dm_npcheck.db/$TABLENAME/$RMDATE 1>>$LOGFILE 2>>$LOGFILE
echo "drop $TABLENAME partition" | tee -a $LOGFILE
exit 0


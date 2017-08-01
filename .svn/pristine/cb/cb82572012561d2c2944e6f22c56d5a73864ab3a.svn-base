#!/bin/bash
if [ $# -ne 2 ]; then
	exit 1
fi

DATE=$1
CITY=$2
TABLENAME=tm_npcheckday_jm
MRCLASS=com.aotain.project.npcheck.NPointCheckDriver
#INPUTPATH=/user/data/test/testnpin
if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi
#DATE=`date -d "$1 day ago"  +%Y%m%d`
OUTPUTPATH=/user/hive/warehouse/aotain_dm_npcheck.db/tm_npcheckday_jm/$DATE
CONFIG=/home/bigdata/project/npcheck/config/hfile-NPointCheck.xml
#WORKOUTPUTPATH=/user/project/output/modynpcheck/
WORKINPUTPATH=/user/hive/warehouse/broadband.db/to_opr_http/$CITY/$DATE
MRPATH=/home/bigdata/project/npcheck/mr
LOGFILE=/home/bigdata/project/npcheck/log/"$TABLENAME"_"$CITY"_"$DATE".log

#copy data
#hadoop fs -cp $OUTPUTPATH/*.txt $WORKINPUTPATH
#hadoop fs -cp $INPUTPATH/*.txt $WORKINPUTPATH
#hadoop fs -mkdir $WORKOUTPUTPATH/$DATE
#WORKOUTPUTPATH=$WORKOUTPUTPATH/$DATE

#mapreduce
echo $MRPATH
echo $MRCLASS
echo $WORKINPUTPATH
echo $WORKOUTPUTPATH
echo $CONFIG
echo $DATE
echo $CITY
hive -e "use aotain_dm_npcheck;alter table $TABLENAME add partition(partdate='$DATE') location '$DATE';" 1>>$LOGFILE 2>>$LOGFILE
hadoop jar $MRPATH/npcheck.jar $MRCLASS $WORKINPUTPATH $OUTPUTPATH $CONFIG $DATE  
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

SDAY=`date -d "$1 day ago"  +%Y%m%d`
echo $SDAY
DAY=$(($1+6))
EDAY=`date -d "$DAY day ago"  +%Y%m%d`
echo $EDAY
#hive -e "use aotain_dm_npcheck;  truncate table tm_npchecksevenDays ; insert into table aotain_dm_npcheck.tm_npchecksevenDays select userid,sum(daycnt)/count(userid) avgcnt,'$SDAY' from aotain_dm_npcheck.tm_npcheckday where  partdate  >='$EDAY' and  partdate  <='$SDAY' group by userid "   1>>$LOGFILE 2>>$LOGFILE

#rm data
DATE=`date -d "9 day ago" +%Y%m%d`
hive -e "use aotain_dm_npcheck;alter table tm_npcheckday drop partition(partdate='$DATE');"
hadoop fs -rm -r /user/hive/warehouse/aotain_dm_npcheck.db/tm_npcheckday/$DATE 1>>$LOGFILE 2>>$LOGFILE

exit 0


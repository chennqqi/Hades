#!/bin/bash


if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

OUTPUTPATH=/user/project/radius
LOGFILE=/home/bigdata/project/push/log/gp_radius_stat.log


#rm data
hadoop fs -rm ${OUTPUTPATH}/* 1>>$LOGFILE 2>>$LOGFILE

hadoop fs -text  /user/hive/warehouse/broadband.db/radius/guangdong/$DATE/* | awk -F "|"  '{if (NF==8 && $3~/^[0-9]*$/) print $1"|"$2"|"$3"|"$4"|"$5"|"$6"|"$7"|"$8}' > /home/bigdata/project/push/data/gpradius

hadoop fs -put  /home/bigdata/project/push/data/gpradius ${OUTPUTPATH}/ 1>>$LOGFILE 2>>$LOGFILE
rm   /home/bigdata/project/push/data/gpradius 1>>$LOGFILE 2>>$LOGFILE

echo `date +"%Y-%m-%d"` " gp_radius $DATE  success!!!  " | tee -a $LOGFILE

exit 0

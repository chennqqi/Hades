#!/bin/bash


if [ $# -ne 1 ]; then
	exit 1
fi


DATE=$1

LOGFILE=/home/bigdata/project/push/log/smartpushpartitions.log


PushLines=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipspush/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print $7","$8}' | sort | uniq`
for temp in $PushLines;
do
        Citycode=`echo $temp | cut -d "," -f 1`
        Partate=`echo $temp | cut -d "," -f 2`
	hive -e "use broadband;alter table push_pushlog add if not exists partition (citycode = '${Citycode}',PartDate='${Partate}') location '${Citycode}/${Partate}';" 1>>${LOGFILE} 2>>${LOGFILE}
	echo `date +"%Y-%m-%d %H:%M:%S"`  "add partition to push_pushlog ${Citycode}/${Partate} "   | tee -a $LOGFILE
done

FeedbackLines=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipsfeedback/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print $7","$8}' | sort | uniq`
for temp in $FeedbackLines;
do
        Citycode=`echo $temp | cut -d "," -f 1`
        Partate=`echo $temp | cut -d "," -f 2`
	hive -e "use broadband;alter table push_feedback add if not exists partition (citycode = '${Citycode}',PartDate='${Partate}') location '${Citycode}/${Partate}';" 1>>${LOGFILE} 2>>${LOGFILE}
	echo `date +"%Y-%m-%d %H:%M:%S"`  "add partition to push_feedback ${Citycode}/${Partate} "   | tee -a $LOGFILE
done

FlowguideLines=`hadoop fs -ls /user/hive/warehouse/broadband.db/ipsflowguide/* | awk -F " " '{if($8!="") print $8}'|  awk -F "/" '{if($8>="'"${DATE}"'")print $7","$8}' | sort | uniq`
for temp in $FlowguideLines;
do
        Citycode=`echo $temp | cut -d "," -f 1`
        Partate=`echo $temp | cut -d "," -f 2`
	hive -e "use broadband;alter table push_flowguide add if not exists partition (citycode = '${Citycode}',PartDate='${Partate}') location '${Citycode}/${Partate}';" 1>>${LOGFILE} 2>>${LOGFILE}
	echo `date +"%Y-%m-%d %H:%M:%S"`  "add partition to push_flowguide ${Citycode}/${Partate} "   | tee -a $LOGFILE
done

echo `date +"%Y-%m-%d"`  " success end!!!  " | tee -a $LOGFILE

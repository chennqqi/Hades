#!/bin/bash


if [ $# -lt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><CITYNAME>"  | tee -a $LOGFILE
	exit 1
fi


#created by turk 2014-12-30 create partition for hive-table
#modified by turk 2015-02-09 
#modified by turk 2015-04-15 v1.4 <drop partition>
#modified by turk 2015-06-09 v1.4 <remove rm cmd,Log File distinction City>
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.4 update(2015-04-14)"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Include create partition"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE


DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/other
CONFIGPATH=${WORKPATH}/config
LOGFILE=${WORKPATH}/log/createpartition_${CITYNAME}_${DATE}.log
echo `date +"%Y-%m-%d %H:%M:%S"`  "param:DATE:$DATE  CITYNAME:$CITYNAME"   | tee -a $LOGFILE


#table
tablename=`cat ${CONFIGPATH}/tablename.ini | wc -l`


#load double table data
for num in $( seq 1 ${tablename} ) 
do
		
		#table name
		FileLine=`cat ${CONFIGPATH}/tablename.ini | sed -n "$num"p`
		
		Flag=`echo ${FileLine} | cut -d : -f 1`
		DualTableName=`echo ${FileLine} | cut -d : -f 2`
		DataPath=`echo ${FileLine} | cut -d : -f 3`
		DeleteDay=`echo ${FileLine} | cut -d : -f 4`
		DATABASE=`echo ${DualTableName} | cut -d . -f 1`
		TABLENAME=`echo ${DualTableName} | cut -d . -f 2`
		
		echo `date +"%Y-%m-%d %H:%M:%S"`  "DataPath:${DataPath}" | tee -a $LOGFILE
			
		echo `date +"%Y-%m-%d %H:%M:%S"`  "TABLENAME:${TABLENAME}" | tee -a $LOGFILE

		if [ ${Flag} = "0" ] && [ -z ${CITYNAME} ]; then
			hive -e "use ${DATABASE};alter table ${TABLENAME} add if not exists partition (PartDate='${DATE}') location '${DATE}';" 1>>${LOGFILE} 2>>${LOGFILE}
			echo `date +"%Y-%m-%d %H:%M:%S"`  "add partition to ${TABLENAME} ${DATE}"   | tee -a $LOGFILE
		elif [ ${Flag} = "1" ] && [ ${CITYNAME} ]; then
			hive -e "use ${DATABASE};alter table ${TABLENAME} add if not exists partition (citycode = '${CITYNAME}',PartDate='${DATE}') location '${CITYNAME}/${DATE}';" 1>>${LOGFILE} 2>>${LOGFILE}
			echo `date +"%Y-%m-%d %H:%M:%S"`  "add partition to ${TABLENAME} ${CITYNAME}/${DATE}"   | tee -a $LOGFILE
			DataPath=${DataPath}${CITYNAME}/
		fi
		
		if [ "$DeleteDay" =  "" ]; then
			continue
		fi

		
		DeleteDay="-${DeleteDay} day"
		declare -i DeleteDate=`date -d "${DeleteDay}" "+%Y%m%d"`
		echo `date +"%Y-%m-%d %H:%M:%S"`  "DataPath:${DataPath}" | tee -a $LOGFILE
		echo `date +"%Y-%m-%d %H:%M:%S"`  "TableName:${TABLENAME}  DeleteDate:${DeleteDate}" | tee -a $LOGFILE
	 
		##write config file
		hadoop fs -ls ${DataPath} | awk '{print $8}' > ${CONFIGPATH}/${CITYNAME}_${DATE}.file

		PartitionNum=`cat ${CONFIGPATH}/${CITYNAME}_${DATE}.file | wc -l`
		
		for pnum in $( seq 1 ${PartitionNum} ) 
		do
			#first line is null
			#pnum=$(($pnum+1))  
			PartitionPath=`cat ${CONFIGPATH}/${CITYNAME}_${DATE}.file | sed -n "$pnum"p`
			declare -i PartitionDate=`echo ${PartitionPath} | awk 'gsub("'${DataPath}'","",$0)'`
				
			echo `date +"%Y-%m-%d %H:%M:%S"`  "TableName:${TABLENAME} PartitionDate:${PartitionDate}  PartitionPath:${PartitionPath}" | tee -a $LOGFILE
			
			if [ "$PartitionPath" =  "" ]; then
				continue
		  	fi
		  	
		  	
		  
			if [ $PartitionDate -lt $DeleteDate ] && [ $PartitionDate -ne 0 ]; then
				if [ ${Flag} = "0" ] && [ -z ${CITYNAME} ]; then
					hive -e "use ${DATABASE};alter table ${TABLENAME} drop if exists partition (PartDate='${PartitionDate}');" >> ${LOGFILE} 2>&1
					hadoop fs -rm -r ${PartitionPath} >> ${LOGFILE} 2>&1
					echo "delete partition ${TABLENAME} ${PartitionDate}" | tee -a $LOGFILE
				elif [ ${Flag} = "1" ] && [ ${CITYNAME} ]; then
					echo "delete partition ${TABLENAME} ${PartitionDate} " | tee -a $LOGFILE
					hive -e "use ${DATABASE};alter table ${TABLENAME} drop if exists partition (citycode = '${CITYNAME}',PartDate='${PartitionDate}');" >> ${LOGFILE} 2>&1
					echo "rm path:${PartitionPath}" | tee -a $LOGFILE
					hadoop fs -rm -r ${PartitionPath} >> ${LOGFILE} 2>&1
				fi
			fi
		done
		
		#rm -f ${CONFIGPATH}/${CITYNAME}_${DATE}.file
done
	
echo "################end##########################" | tee -a $LOGFILE
exit 0
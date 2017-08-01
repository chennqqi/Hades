#!/bin/bash

#Created by turk 2015-07-13
#Project  Terminal V2

DATE=$1
DAY=$2
WORKPATH=/home/bigdata/project/terminal
LOGFILE=$WORKPATH/log/Terminal_V2_"$DATE".log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Project Terminal V2--------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 2.0 update(2015-07-13)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><DAY>"  | tee -a $LOGFILE
	exit 1
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar

HOST=172.16.1.35

#HDFS
OUTPUTPATH=/user/project/input/terminal

TIME=`echo "partdate = '"$DATE"'"`

if [ "$DAY" = "1" ]; then
	TERM_TABLENAME=aotain_dm_terminalstat.tm_terminal_info
	PC_TABLENAME=aotain_dm_npcheck.tm_npcheckday
	SUFFIX=day
elif [ "$DAY" = "7" ]; then
	TERM_TABLENAME=aotain_dm_terminalstat.tm_terminal_info_7day
	PC_TABLENAME=aotain_dm_terminalstat.tm_mbpcanalysis_7day
	SUFFIX=7day
elif [ "$DAY" = "15" ]; then
	TERM_TABLENAME=aotain_dm_terminalstat.tm_terminal_info_15day
	PC_TABLENAME=aotain_dm_terminalstat.tm_mbpcanalysis_15day
	SUFFIX=15day
fi


#-------------aotain_dm_terminalstat.tm_terminal_versionstat_day
###IMPORT ORACLE
hadoop fs -rm -r ${OUTPUTPATH}/tm_terminal_versionstat_${SUFFIX}/*
hive -e "insert overwrite directory '${OUTPUTPATH}/tm_terminal_versionstat_${SUFFIX}'
select partdate reportdate,TERMINALID,TERMINAL_CNT,USER_CNT,TERMINAL_CNT/sum(TERMINAL_CNT) over(partition by partdate)*100 TERMINAL_RATE 
from (
select ${DATE} partdate,fullname TERMINALID,sum(devicecount) TERMINAL_CNT,count(distinct username) USER_CNT 
from ${TERM_TABLENAME} where ${TIME} and citycode = 'shenzhen' and fullname <> '' 
group by fullname) t" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

sqoop export --connect jdbc:oracle:thin:@${HOST}:1521:BSMP --username infi --password infi2015 --table tm_terminal_versionstat_${SUFFIX} --columns reportdate,terminalid,terminal_cnt,user_cnt,terminal_rate --export-dir ${OUTPUTPATH}/tm_terminal_versionstat_${SUFFIX} --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

#-------------aotain_dm_terminalstat.tm_user_stat_day
###IMPORT ORACLE
##------------------PHONE/PAD--------------------
hadoop fs -rm -r ${OUTPUTPATH}/tm_user_stat_${SUFFIX}/*
hive -e "insert overwrite directory '${OUTPUTPATH}/tm_user_stat_${SUFFIX}'
    select ${DATE} REPORTDATE,userid USERACCOUNT,'PC' TYPEID,'' TERMINALID,daycnt TERMINAL_CNT from ${PC_TABLENAME}
         where ${TIME}
	union all     
	select ${DATE} REPORTDATE,username USERACCOUNT,'Phone' TYPEID,fullname TERMINALID,devicecount TERMINAL_CNT 
	        from ${TERM_TABLENAME} where citycode = 'shenzhen' and ${TIME} and fullname <> ''" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

sqoop export --connect jdbc:oracle:thin:@${HOST}:1521:BSMP --username infi --password infi2015 --table tm_user_stat_${SUFFIX} --columns reportdate,useraccount,typeid,terminalid,terminal_cnt --export-dir ${OUTPUTPATH}/tm_user_stat_${SUFFIX} --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

#-------------aotain_dw.tm_terminal_totalstat_day
###IMPORT ORACLE
hadoop fs -rm -r ${OUTPUTPATH}/tm_terminal_totalstat_${SUFFIX}/*
hive -e "insert overwrite directory '${OUTPUTPATH}/tm_terminal_totalstat_${SUFFIX}'
        select r1.*,case when terminal_cnt = 0 then 0 else terminal_cnt/sum(terminal_cnt) over() *100 end TERMINAL_RATE from (
			select ${DATE} REPORTDATE,'PC' TYPEID,case when sum(daycnt) is null then 0 else sum(daycnt) end TERMINAL_CNT,count(distinct userid) USER_CNT
	from ${PC_TABLENAME} where ${TIME}
			union all
			select ${DATE} REPORTDATE,'Phone' TYPEID,case when sum(devicecount) is null then 0 else sum(devicecount) end TERMINAL_CNT,count(distinct username) USER_CNT
			from ${TERM_TABLENAME} 
			where citycode = 'shenzhen' and ${TIME} and fullname <> '' ) r1" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

sqoop export --connect jdbc:oracle:thin:@${HOST}:1521:BSMP --username infi --password infi2015 --table tm_terminal_totalstat_${SUFFIX} --columns reportdate,typeid,terminal_cnt,user_cnt,terminal_rate --export-dir ${OUTPUTPATH}/tm_terminal_totalstat_${SUFFIX} --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi
  
#----ORACLE---PROC----
#if [ "$DAY" = "1" ]; then
SERVICENAME=bsmp
HOST=${HOST}
USER=infi         
PASSWD=infi2015
TNSNAME=BSMP
CONNURL="${USER}/${PASSWD}@${TNSNAME}"
	
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Oracle Proc" | tee -a $LOGFILE
sqlplus -S "${CONNURL}" <<!
exec sp_tm_terminal_dimstat_day(${DATE},${DAY});
exit;
!
#fi
 
echo `date +"%Y-%m-%d %H:%M:%S"`      "Load data Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

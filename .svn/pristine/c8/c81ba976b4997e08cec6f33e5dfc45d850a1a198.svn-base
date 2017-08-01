#!/bin/bash

#Created by turk 2014-12-25
#Project  Terminal
#Version 1.3 modified by turk <add 7day 15day stat>
#															drop table
#Version 1.4 modified by turk <delete history file>
#Version 1.5 modified by turk <device -> fullname>

DATE=$1
DAY=$2
WORKPATH=/home/bigdata/project/terminal
LOGFILE=$WORKPATH/log/Terminal_"$DATE".log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Project  Terminal--------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.4 update(2015-04-10)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><DAY>"  | tee -a $LOGFILE
	exit 1
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar

HOST=121.14.127.68

#HDFS
OUTPUTPATH=/user/project/input/terminal

if [ "$DAY" = "1" ]; then
	TIME=`echo "partdate = '"$DATE"'"`
	SUFFIX=day
elif [ "$DAY" = "7" ]; then
	DATE1=`date -d "${DATE}" +%s`
	DAY7=604800
	DATE2=$(($DATE1-$DAY7))
	STARTDATE=`date -d @$DATE2 +%Y%m%d`
	ENDDATE=$DATE
	TIME=`echo "partdate >= '"$STARTDATE"' and partdate <= '"$ENDDATE"' "`
	SUFFIX=7day
elif [ "$DAY" = "15" ]; then
	DATE1=`date -d "${DATE}" +%s`
	DAY15=1296000
	DATE2=$(($DATE1-$DAY15))
	STARTDATE=`date -d @$DATE2 +%Y%m%d`
	ENDDATE=$DATE
	TIME=`echo "partdate >= '${STARTDATE}' and partdate <= '${ENDDATE}' "`
	SUFFIX=15day
fi


#-------------aotain_dm_terminalstat.tm_terminal_versionstat_day
###IMPORT ORACLE
hadoop fs -rm -r ${OUTPUTPATH}/tm_terminal_versionstat_${SUFFIX}/*
hive -e "insert overwrite directory '${OUTPUTPATH}/tm_terminal_versionstat_${SUFFIX}'
select a.date reportdate,a.model TERMINALID,a.num TERMINAL_CNT,b.num USER_CNT,
a.num/sum(a.num)over(partition by a.date)*100 TERMINAL_RATE
from (select '${DATE}' date,fullname model,count(distinct username,fullname,opersys,opersysver) num from aotain_dw.tw_terminal_info t where ${TIME} and citycode = 'shenzhen' 
        and fullname <> '' group by fullname ) a join (select date,model,count(*) num from
(select distinct '${DATE}' date,username,fullname model
from aotain_dw.tw_terminal_info where ${TIME} and citycode = 'shenzhen' and fullname <> '') r1
group by model,date) b on a.date = b.date and a.model = b.model" 1>>$LOGFILE 2>>$LOGFILE
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
hadoop fs -rm -r ${OUTPUTPATH}/tm_user_stat_${SUFFIX}/*
hive -e "insert overwrite directory '${OUTPUTPATH}/tm_user_stat_${SUFFIX}'
        select '${DATE}' REPORTDATE,username USERACCOUNT,'PC' TYPEID,fullname TERMINALID,count(distinct opersys,opersysver) TERMINAL_CNT 
        from aotain_dw.tw_terminal_info where ${TIME} and citycode = 'shenzhen' and opersys = 'Windows' and opersysver<>'CE' 
        group by username,fullname 
union all     
select '${DATE}' REPORTDATE,username USERACCOUNT,'Phone' TYPEID,fullname TERMINALID,count(distinct fullname,username,opersys,opersysver) TERMINAL_CNT 
        from aotain_dw.tw_terminal_info where ${TIME} and citycode = 'shenzhen' and devicename <> '' group by username,fullname" 1>>$LOGFILE 2>>$LOGFILE
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
        select reportdate,typeid,TERMINAL_CNT,
        USER_CNT,
        case when TERMINAL_CNT = 0 then 0 else TERMINAL_CNT/sum(TERMINAL_CNT) over() *100 end TERMINAL_RATE from (
        select '${DATE}' REPORTDATE,'PC' TYPEID,count(distinct opersys,opersysver,username) TERMINAL_CNT,count(distinct username) USER_CNT from aotain_dw.tw_terminal_info t
        where $TIME and citycode = 'shenzhen' and opersys = 'Windows' and opersysver<>'CE'
        union all
        select '${DATE}' REPORTDATE,'Phone' TYPEID,count(distinct devicename,username,opersys,opersysver) TERMINAL_CNT,count(distinct username) USER_CNT
        from aotain_dw.tw_terminal_info t where $TIME and citycode = 'shenzhen' and devicename <> '' ) r" 1>>$LOGFILE 2>>$LOGFILE
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
TNSNAME=TS
CONNURL="${USER}/${PASSWD}@${TNSNAME}"
	
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Oracle Proc" | tee -a $LOGFILE
sqlplus -S "${CONNURL}" <<!
exec sp_tm_terminal_dimstat_day(${DATE},${DAY});
exit;
!
#fi
 
echo `date +"%Y-%m-%d %H:%M:%S"`      "Load data Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

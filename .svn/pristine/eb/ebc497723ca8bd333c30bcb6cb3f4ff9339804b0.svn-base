#!/bin/bash

#Created by turk 2015-03-26
#Project  N+1 data import
#Version 1.0 modified by turk 

DATE=$1
CITYCODE=$2
WORKPATH=/home/bsmp/work
LOGFILE=/home/bigdata/project/npcheck/log/sqoop_to_oracle_"$DATE".log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------N+1   DATA IMPORT--------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.0 update(2015-03-26)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE><CITYCODE>"  | tee -a $LOGFILE
	exit 1
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar

#HDFS
OUTPUTPATH=/user/project/input/npcheck

STARTDATE=`date -d "7 day ago" +%Y%m%d`

SERVICENAME=bsmp
HOST1=121.15.207.184 
HOST2=121.15.207.185 
USER=share001            
PASSWD=share001
TABLENAME=sharev5_check_fromhadoop
TNS="(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST =$HOST1)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = $HOST2)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = ${SERVICENAME})(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 2))))"
CONNURL="${USER}/${PASSWD}@'${TNS}'"
CONNSTR="$USER/$PASSWD@'$TNS'"

TruncateRecord()
{
sqlplus -s "$CONNURL" <<!
set timing on;
truncate table sharev5_check_fromhadoop;
exit;
!
}

TruncateRecord

echo $STARTDATE
echo $DATE

#rm data
hadoop fs -rm ${OUTPUTPATH}/sharev5_check_fromhadoop/*

###IMPORT ORACLE
hive -e "insert overwrite directory '${OUTPUTPATH}/sharev5_check_fromhadoop'
        select userid,max(pccnt) pccnt,max(mbcnt) mbcnt,'${DATE}' pn_date from
(select userid,case when sum(daycnt)/count(*)<1 then 1 else floor(sum(daycnt)/count(*)) end pccnt,0 mbcnt
        from aotain_dm_npcheck.tm_npcheckday
        where partdate >= '${STARTDATE}' and partdate <= '${DATE}'
        group by userid
union all
select userid,0 pccnt,case when sum(devicecount)/count(*)<1 then 1 else floor(sum(devicecount)/count(*)) end mbcnt
        from aotain_dm_npcheck.tm_mbcheck
        where partdate >= '${STARTDATE}' and partdate <= '${DATE}'
        group by userid) a
group by userid" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

sqoop export --connect jdbc:oracle:thin:@"${TNS}" --username ${USER} --password ${PASSWD} --table ${TABLENAME} --columns USERID,PCCNT,MBCNT,PN_DATE --export-dir ${OUTPUTPATH}/sharev5_check_fromhadoop --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Load data Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE
	
exit 0

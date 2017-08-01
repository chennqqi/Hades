#!/bin/bash
#

WORKPATH=/home/bsmp/work/


export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar
#ORACLE
SERVICENAME=bsmp
OUTPUTPATH=/data02/ubascheck
HFDSPATH=/user/project/output/npcheckdetail
HOST1=121.15.207.184 
HOST2=121.15.207.185 
USERNAME=share001            
PASSWD=share001
TABLENAME=sharev5_check_fromhadoop
TNS="(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST =$HOST1)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = $HOST2)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = ${SERVICENAME})(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 2))))"
CONNURL="${USER}/${PASSWD}@'${TNS}'"
CONNSTR="$USER/$PASSWD@'$TNS'"

#HDFS
rm -f $OUTPUTPATH/*
DATE=$1
if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
	STARTDATE=`date -d "8 day ago" +%Y%m%d`
fi

hadoop fs -rm -r $HFDSPATH
				
sqoop import --connect jdbc:oracle:thin:@"${TNS}" --username ${USERNAME} --password ${PASSWD} --query "select * from (select '$DATE' npdate,user_name,max(pccnt) pccnt,max(pcavg) pcavg,max(pcmax) pcmax,
max(mbcnt) mbcnt,max(mbavg) mbavg,max(mbmax) mbmax from
(select user_name,0 pccnt,floor(sum(curr_count)/count(*)) pcavg,max(curr_count) pcmax,
0 mbcnt,floor(sum(mtfinal_count)/count(*)) mbavg,max(mtfinal_count) mbmax
from sharev5_check_log a 
group by user_name
union all
select user_name,curr_count,0,0,mtfinal_count,0,0 from sharev5_check_log
where log_time = to_date('$DATE','yyyy-mm-dd'))
group by user_name) where  \$CONDITIONS " -m 1 --target-dir $HFDSPATH

hadoop fs -get  $HFDSPATH/part*  ${OUTPUTPATH}/
#mergefile
cat $OUTPUTPATH/part* > $OUTPUTPATH/1_N"$DATE"0000_MONTH.csv
gzip $OUTPUTPATH/1_N"$DATE"0000_MONTH.csv

rm -f $OUTPUTPATH/part*
#ftpput
ftp -i -n 219.134.184.76 <<END
user aotian shenzhen755
cd 1ncheck
lcd ${OUTPUTPATH}/
bin
put *
bye

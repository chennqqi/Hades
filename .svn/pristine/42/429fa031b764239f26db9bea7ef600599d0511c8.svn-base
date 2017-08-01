#!/bin/bash
#

WORKPATH=/home/bsmp/work/


export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar
#ORACLE
SERVICENAME=bsmp
OUTPUTPATH=/data02/1nblacklist
HFDSPATH=/user/project/output/npcheckdetail/1nblacklist
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
				
sqoop import --connect jdbc:oracle:thin:@"${TNS}" --username ${USERNAME} --password ${PASSWD} --query "select * from (select user_name,permit_num,ctrl_type
from sharev5_user_info
where user_status = 2
) where  \$CONDITIONS " -m 1 --target-dir $HFDSPATH

hadoop fs -get  $HFDSPATH/part*  ${OUTPUTPATH}/
#mergefile
cat $OUTPUTPATH/part* > $OUTPUTPATH/IBSS"$DATE"0000_MONTH.csv
gzip $OUTPUTPATH/IBSS"$DATE"0000_MONTH.csv

rm -f $OUTPUTPATH/part*
#ftpput
ftp -i -n 219.134.184.76 <<END
user aotian shenzhen755
cd 1nblacklist
lcd ${OUTPUTPATH}/
bin
put *
bye

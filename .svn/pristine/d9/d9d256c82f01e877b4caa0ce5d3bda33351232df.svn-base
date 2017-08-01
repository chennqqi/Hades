#!/bin/bash

#Created by turk 2015-03-11
#Project  Terminal
#Version 1.0
#Target Table:tm_portal_phone_day/tm_portal_userhttp_day/tm_portal_userhttp_hour
#

DATE=$1
WORKPATH=/home/bsmp/work/
LOGFILE=$WORKPATH/log/portal_turk_${DATE}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-----------Portal           --------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Shell Version 1.1 update(2015-03-11)" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------------------------------------" | tee -a $LOGFILE


if [ $# -lt 1 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE>"  | tee -a $LOGFILE
	exit 1
fi

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${WORKPATH}/lib/ojdbc14.jar
#ORACLE
IP=172.16.1.35
SERVICENAME=BSMP
USERNAME=infi
PASSWD=infi2015
#HDFS
OUTPUTPATH=/user/hive/warehouse/aotain_dm_neblaportal.db

hive -e "insert overwrite directory '${OUTPUTPATH}/tm_portal_phone_day' 
				select '${DATE}' reportdate,cityname,teloperator,sum(local_cnt) local_cnt,sum(other_cnt) other_cnt from (			
				select distinct attvalue phone,c.cityname,case when b.cityname = c.cityname then 1 else 0 end local_cnt,			
				case when b.cityname <> c.cityname then 1 else 0 end other_cnt,			case when b.card_type like '%联通%' then '联通' 			
				when b.card_type like '%电信%' then '电信' 			when b.card_type like '%移动%' then '移动'end teloperator			
				from aotain_dw.tw_user_postinfo a left join aotain_dim.to_ref_mobile_area b			
				on substring(a.attvalue,0,7) = b.mobile left join aotain_dim.to_ref_cityinfo c on a.citycode = c.citycode			
				where attcode in ('Phone','Telephone')) r 			
				where r.teloperator is not null and r.cityname is not null group by cityname,teloperator" 1>>$LOGFILE 2>>$LOGFILE	
				
sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table tm_portal_phone_day --columns reportdate,citycode,teloperator,local_cnt,other_cnt --export-dir ${OUTPUTPATH}/tm_portal_phone_day --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE

if [ $? -ne 0 ]
then
     echo `date +"%Y-%m-%d %H:%M:%S"`  "exec shell failed"  | tee -a $LOGFILE
     exit 1
fi

hive -e "insert overwrite directory '${OUTPUTPATH}/tm_portal_userhttp_day'
			select partdate,citycode,device,case when count(distinct username) is null then 0 else count(distinct username) end uv_cnt,case when count(*) is null then 0 else count(*) end pv_cnt
	from broadband.to_opr_http where partdate = '${DATE}' and device is not null
	group by partdate,citycode,device" 1>>$LOGFILE 2>>$LOGFILE
			
sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table tm_portal_userhttp_day --columns reportdate,citycode,t_type,uv_cnt,pv_cnt --export-dir ${OUTPUTPATH}/tm_portal_userhttp_day --fields-terminated-by '\001' -m 2 1>>$LOGFILE 2>>$LOGFILE

if [ $? -ne 0 ]
then
     echo `date +"%Y-%m-%d %H:%M:%S"`  "exec shell failed"  | tee -a $LOGFILE
     exit 1
fi

hive -e "insert overwrite directory '${OUTPUTPATH}/tm_portal_userhttp_hour'
			select * from (select partdate,from_unixtime(AccessTime,'yyyyMMddHH') reportdate,citycode,device,case when count(distinct username) is null then 0 else count(distinct username) end  uv_cnt,case when count(*) is null then 0 else count(*) end pv_cnt 
	from broadband.to_opr_http where partdate = '${DATE}' and device is not null
	group by from_unixtime(AccessTime,'yyyyMMddHH'),citycode,device,partdate) r" 1>>$LOGFILE 2>>$LOGFILE
			
sqoop export --connect jdbc:oracle:thin:@${IP}:1521:${SERVICENAME} --username ${USERNAME} --password ${PASSWD} --table tm_portal_userhttp_hour --columns partdate,reportdate,citycode,t_type,uv_cnt,pv_cnt --export-dir ${OUTPUTPATH}/tm_portal_userhttp_hour --fields-terminated-by '\001' -m 2  --input-null-string '\\N'  --input-null-non-string '\\N' 1>>$LOGFILE 2>>$LOGFILE

if [ $? -ne 0 ]
then
     echo `date +"%Y-%m-%d %H:%M:%S"`  "exec shell failed"  | tee -a $LOGFILE
     exit 1
fi

echo `date +"%Y-%m-%d %H:%M:%S"`      "Load data Success" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE
 

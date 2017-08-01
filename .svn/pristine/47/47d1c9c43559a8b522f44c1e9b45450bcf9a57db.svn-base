#!/bin/bash
############ parse command line args ############################

if [ $# -ne 1 ]; then
        exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

############# database config  ##############################
tnsName=bsmp
hostname1=121.15.207.184                     #172.16.1.35
hostname2=121.15.207.185                     #
#!/bin/bash
############ parse command line args ############################

if [ $# -ne 1 ]; then
        exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

############# database config  ##############################
tnsName=bsmp
hostname1=121.15.207.184                     #172.16.1.35
hostname2=121.15.207.185                     #
user=share001                                #infi
passwd=share001                              #infi2015
detailTabname=sharev5_check_log
realtimeTabname=sharev5_month_record
database=$user/$passwd@$tnsName
LOGFILE=/home/bigdata/project/npcheck/log/"$detailTabname"_"$DATE".log

dbconn="(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST =$hostname1)(PORT = 1521))(ADDRESS = (PROTOCOL = TCP)(HOST = $hostname2)(PORT = 1521))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = bsmp)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 2))))"
#dbconn="(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = $hostname1)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = bsmp)))"
connStr="$user/$passwd@'$dbconn'"
#echo $connStr

##########    execute sql file      ###########
#sqlplus -s ${database}<<EOF
#start /tmp/${tablename}.sql
#quit;

########## execute sql statments ###########
  # single host the right connect way like below
  # sqlplus $user/$passwd@$hostname1:1521/bsmp <<!

##########  business process    #############
mergeCheckLog()
{
sqlplus -s "$connStr" <<!
set timing on;

insert into sharev5_check_log
select a.user_name,a.district_code,a.user_type,a.user_status,0,
to_date(b.pn_date||'000000','yyyy-mm-dd hh24:mi:ss'),
to_date(b.pn_date||'235959','yyyy-mm-dd hh24:mi:ss'),
b.pccnt,0,0,0,to_date(b.pn_date,'yyyy-mm-dd'),a.userid,b.mbcnt
from sharev5_user_info a join sharev5_check_fromhadoop b
on a.user_name = b.userid
where b.pn_date='$DATE';

commit;
exit;
!
}


mergeMonthRecord()
{
sqlplus -s "$connStr" <<!
set timing on;

MERGE INTO sharev5_month_record b
 USING (select c.user_name,c.district_code,c.user_type,c.user_status,c.userid,a.pccnt,a.mbcnt,a.pn_date
  from sharev5_check_fromhadoop a,sharev5_user_info c
 where a.userid = c.user_name) a
    ON (a.user_name = b.user_name)
    WHEN MATCHED THEN
    UPDATE
     SET b.final_count = a.pccnt,b.mtfinal_count = a.mbcnt,b.user_status=a.user_status,
     b.max_count=case when a.pccnt>b.max_count then a.pccnt
     else b.max_count end,
     b.mtmax_count=case when a.mbcnt>b.mtmax_count then a.mbcnt
     else b.mtmax_count end,
     b.modify_time = to_date(a.pn_date||'000000','yyyy-mm-dd hh24:mi:ss')
    WHEN NOT MATCHED THEN
     INSERT
     values(a.user_name,a.district_code,a.user_type,
     a.user_status,a.pccnt,a.pccnt,
     to_date(a.pn_date||'000000','yyyy-mm-dd hh24:mi:ss'),
     to_date(a.pn_date||'000000','yyyy-mm-dd hh24:mi:ss'),
     a.userid,a.mbcnt,a.mbcnt);

commit;

exit;
!
}

########## business dipatch ##################
mergeCheckLog 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

mergeMonthRecord 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0


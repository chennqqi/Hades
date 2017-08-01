#!/bin/bash


#Created by turk 2015-03-02
#Version 1.0 Shenzhen telecom Other operator

if [ $# -lt 2 ]; then
	echo `date +"%Y-%m-%d %H:%M:%S"`  "Usage:<DATE> <CITYNAME>"
	exit 1
fi

DATE=$1
CITYNAME=$2

WORKPATH=/home/bigdata/project/other
JAVALIB=${WORKPATH}/mr/
LOGFILE=${WORKPATH}/log/other_operator_${DATE}_${CITYNAME}.log

echo `date +"%Y-%m-%d %H:%M:%S"`  "------Shenzhen telecom Other operator------------" | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "------Version 1.0 update(2015-03-02)-------------"     | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-------------------------------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "-------------------------------------------------"  | tee -a $LOGFILE
echo `date +"%Y-%m-%d %H:%M:%S"`  "Start..."  | tee -a $LOGFILE



LOCALOUTPUTPATH=/data02/sztelecom

hive -e "insert overwrite local directory '${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}'
	row format delimited
	fields terminated by ','
	select b.id,b.classname,b.url,b.title,a.username,count(*) num
	from broadband.to_opr_http a join aotain_dim.to_ref_otherurl_sztelecom b 
	On a.url = b.url where a.partdate = '${DATE}' and citycode = '${CITYNAME}' 
	group by b.id,b.classname,b.url,b.title,a.username" 1>>$LOGFILE 2>>$LOGFILE
ret=$?
if [ ${ret} -ne 0 ]; then
        exit ${ret}
fi

cat ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/* >> ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/userlist_${CITYNAME}_${DATE}.txt

#delete history file
OLDDATE=`date -d "10 day ago"   +%Y%m%d`
rm -rf ${LOCALOUTPUTPATH}/${CITYNAME}/${OLDDATE}

#upload ftp
ftp -i -n <<FTPIT
open 121.15.207.113
user ubasftp 557744
bin
lcd `dirname ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/userlist_${CITYNAME}_$DATE.txt`
put `basename ${LOCALOUTPUTPATH}/${CITYNAME}/${DATE}/userlist_${CITYNAME}_$DATE.txt`
quit                                                     
FTPIT
#----------------ftp end-------------------------

echo `date +"%Y-%m-%d %H:%M:%S"`      "Exec Success [$DATE]!" | tee -a $LOGFILE

exit 0


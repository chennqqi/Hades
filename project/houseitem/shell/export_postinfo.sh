#!/bin/bash
#Created by gqy 2015-04-02
#export table tw_user_postinfo

if [ $# -ne 3 ]; then
        exit 1
fi

DATE=$1
CITYCODE=$2
TYPECODE=$3

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi

typeset -l VARIABLE
VARIABLE=$TYPECODE

TABLENAME=tw_user_postinfo
LOGFILE=/home/bigdata/project/houseitem/log/"$TABLENAME".log
#OUTPUTPATH=/data02/export_postinfo/"$TYPECODE"_"$CITYCODE"_"$DATE"
OUTPUTPATH=/data02/export_postinfo
UPLOADPATH=/aotainftp/${VARIABLE}
echo $OUTPUTPATH
echo "upload file ftp path: $UPLOADPATH"

#rmdata
rm -f $OUTPUTPATH/*

hive -e "use aotain_dw;
insert overwrite local directory '$OUTPUTPATH' row format delimited fields terminated by '|' select userid,attvalue,lastdate,citycode from $TABLENAME
where citycode = '$CITYCODE' and attcode ='$TYPECODE' and lastdate = '$DATE';" 1>>$LOGFILE  2>>$LOGFILE;

cat $OUTPUTPATH/* > $OUTPUTPATH/"$TYPECODE"_"$CITYCODE"_"$DATE".txt

#rm
rm -rf $OUTPUTPATH/0*

#ftpput
ftp -v -n 172.16.1.38 <<END
user upfile  e%erVuFTc)V3
cd  $UPLOADPATH
lcd ${OUTPUTPATH}/
put "$TYPECODE"_"$CITYCODE"_"$DATE".txt
bye
exit 0

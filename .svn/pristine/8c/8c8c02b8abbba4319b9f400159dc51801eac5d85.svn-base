#!/bin/bash

#Created by liujq 2015-04-13
#create to_ref_keyclass from  tw_fact_useracctag and to_url_class

if [ $# -ne 1 ]; then
	exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date -d "1 day ago" +%Y%m%d`
fi


TABLENAME=to_ref_keyclass
LOGFILE=/home/bigdata/project/premark/log/"$TABLENAME"_"$DATE".log

#get new day all keywords into  tmp_refkeyclass_incre
hive -e "set mapred.job.name = premark_keyclass_"$DATE"data;
insert overwrite table aotain_dw.tmp_refkeyclass_incre
select distinct tag.keyword ,url.class_id ,url.class_name,url.parent_id,url.parent_name 
from aotain_dw.tw_fact_useracctag tag join aotain_dim.to_url_class url on cast(tag.ckeytag  as bigint) = url.class_id
where length(trim(tag.ckeytag)) > 0 and partdate=$DATE;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

#increase data into to_ref_keyclass
hive -e "set mapred.job.name = premark_ref_keyclassincr_"$DATE";
insert overwrite table aotain_dim.to_ref_keyclass
select t.keyword,t.classid,t.classname,t.parentid,t.parentname from aotain_dim.to_ref_keyclass t
left outer join aotain_dw.tmp_refkeyclass_incre tp on (t.keyword=tp.keyword) where tp.keyword is null
union all 
select keyword,classid,classname,parentid,parentname from aotain_dw.tmp_refkeyclass_incre;" 1>>$LOGFILE 2>>$LOGFILE

ret=$?
if [ $ret -ne 0 ]; then
        exit $ret
fi

exit 0

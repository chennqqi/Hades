#!/bin/sh
# ===================================================================================  
# Filename:    szanalysis_v2.0_exportctnetclassreport.sh
# Revision:    2.0
# Date:        2015/0/06
# Author:      zhengsh
# Description: Analysis of demand in Shenzhen
# Notes:       szanalysis_v2.0_exportctnetclassreport.sh 00 day
# ===================================================================================


AOTAIN_SZUSERANALYSIS_LOG=/home/nebula/log/szanalysis_v2.0/szuseranalysis.log
OUTPUTPATH=/home/nebula/outdata/szanalysis_v2.0

######################
# log data to file 
######################
function fecho()
{
    echo -e `date +"%Y%m%d%H%M%S"` $@>>${AOTAIN_SZUSERANALYSIS_LOG}
}


#########################
# main process function #
#########################
function main()
{
 
    # define parameters    
    OP_EDAY=`date +"%Y%m%d" --date="-1 day"`
    OP_TABLE_NAME=tmp_day_ctnetclass
    FILENAME="CTNETVisitsClasses_${OP_EDAY}.txt"
    rm -rf ${OUTPUTPATH}/${FILENAME}

    fecho "Gets the ${OP_EDAY} execution log of szanalysis_v2.0_exportctnetclassreport.sh"

    # ready to create table and read data
    /usr/bin/hive -S -e "use aotain_dw; drop table if exists ${OP_TABLE_NAME};"
    /usr/bin/hive -S -e "use aotain_dw; create table ${OP_TABLE_NAME} as
select * from (select zz.userid,zz.parent_name,yy.class_name,zz.tt1,yy.tt2,zz.rnop,row_number() over(partition by yy.userid, yy.parent_name order by yy.tt2 desc) as rno
          from (select userid, parent_name, tt1,rnop from (select userid,parent_name,row_number() over(partition by bb.userid order by tt1 desc) as rnop,tt1 from (select t.userid, parent_name, sum(frequency) tt1 from tw_static_areauseracc t
                                  join (select class_id, parent_name from aotain_dim.to_url_class where parent_id is not null group by class_id, parent_name) g
                                    on t.attvalue = g.class_id
                                 where citycode = 'shenzhen' and partdate = '${OP_EDAY}' and attcode = 'URLNo' and attvalue != 0  group by t.userid, parent_name) bb) kk
                 where rnop < 4) zz
          left join (select t.userid,parent_name,class_name,sum(frequency) tt2 from tw_static_areauseracc t join (select class_id, parent_name, class_name from aotain_dim.to_url_class where parent_id is not null group by class_id, parent_name, class_name) g
                        on t.attvalue = g.class_id
                     where citycode = 'shenzhen' and partdate = '${OP_EDAY}' and attcode = 'URLNo' and attvalue != 0 group by t.userid, parent_name, class_name) yy
            on zz.userid = yy.userid and zz.parent_name = yy.parent_name) hh
 where rno < 4;"

    /usr/bin/hive -S -e "use aotain_dw;select CONCAT_WS('|','${OP_EDAY}',userid,regexp_replace(regexp_replace(CONCAT_WS(',',ds2,case when ds2[0] is null then '0|0|0|0' end, case when ds2[1] is null then '0|0|0|0' end, case when ds2[2] is null then '0|0|0|0' end), '1,|2,|3,', ''),',','|') ) from (select userid, sort_array(collect_set(CONCAT_WS(',', cast(rnop as String), parent_name, clist))) ds2 from (select userid, parent_name, rnop, regexp_replace(CONCAT_WS(',', ds, case when ds [ 0 ] is null then '0' end, case when ds [ 1 ] is null then'0' end, case when ds [ 2 ] is null then '0' end),'1_|2_|3_','') clist from (select userid, parent_name, rnop,sort_array(collect_set(CONCAT_WS('_',cast(rnop as String), cast(rno as String), class_name))) ds from ${OP_TABLE_NAME} ta group by userid, parent_name, rnop) tr1) tr2  group by userid) tr3;">${OUTPUTPATH}/${FILENAME}".WRITING"

    mv ${OUTPUTPATH}/${FILENAME}".WRITING" ${OUTPUTPATH}/${FILENAME}
    #scp ${OUTPUTPATH}/${FILENAME} root@172.16.1.38:/home/nebula/outdata/szanalysis_v2.0
    #mv ${OUTPUTPATH}/${FILENAME} /home/nebula/outdata/bak/szanalysis_v2.0/${FILENAME}

    fecho "Table ${OP_TABLE_NAME} build completed"
    /usr/bin/hive -S -e "use aotain_dw; drop table if exists ${OP_TABLE_NAME};"
    return 0
} 

######################
#    main procedure
######################
main "$@"
exit $?

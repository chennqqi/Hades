#!/bin/sh
# ===================================================================================  
# Filename:    szanalysis_v2.0_unitofctwapreport.sh
# Revision:    2.0
# Date:        2015/0/06
# Author:      zhengsh
# Description: Analysis of demand in Shenzhen
# Notes:       szanalysis_v2.0_unitofctwapreport.sh 00 day
# ===================================================================================


AOTAIN_SZUSERANALYSIS_LOG=/home/nebula/log/szanalysis_v2.0/szuseranalysis.log

######################
# log data to file 
######################
function fecho()
{
    echo -e `date +"%Y%m%d%H%M%S"` $@>>${AOTAIN_SZUSERANALYSIS_LOG}
}

function getlastday(){
    year=`expr substr $1 1 4`
    month=`expr substr $1 5 2`
    if [ $month = '01' ] || [ $month = '03' ] || [ $month = '05' ] || [ $month = '07' ] || [ $month = '08' ] || [ $month = '10' ] || [ $month = '12' ] ; then
        return $year''$month''31
    elif [ $month = '02' ] ; then
        if [ `expr $year % 400` = 0 ] ; then
            return $year''$month''29
        elif [ `expr $year % 4` = 0 ] && [ `expr $year % 100` != 0 ] ; then
            return $year''$month''29
        else
            return $year''$month''28
        fi
    else
        return $year''$month''30
    fi
}

#########################
# main process function #
#########################
function main()
{
    echo "Validate parameters..."
    # validate parameters
    if [ $# -ne 2 ]; then
        echo "Invalid cmd format, correct format is as following:"
        echo "eg, ./szanalysis_v2.0_unitofctwapreport.sh 00 day"
        return 1;
    fi

    # define parameters    
    OP_EDAY=`date +"%Y%m%d" --date="-1 day"`
    OP_TABLE_NAME=tmp_hour_ctwapvisits_$1
    if [ "$2" = "month" ]; then
        premonth=`date -d last-month +%Y%m`
        OP_SDAY=${premonth}''01
        OP_EDAY=getlastday ${premonth}
    elif [ "$2" = "week" ]; then
        OP_SDAY=`date +"%Y%m%d" --date="-7 day"`
    else
        OP_SDAY=`date +"%Y%m%d" --date="-6 day"`
    fi
    fecho "Gets the ${OP_SDAY} - ${OP_EDAY} execution log of gethourreport.sh"

    # ready to create table and read data
    /usr/bin/hive -S -e "use aotain_dw; drop table if exists ${OP_TABLE_NAME};"
    /usr/bin/hive -S -e "use aotain_dw; create table ${OP_TABLE_NAME} as  select userid,regexp_replace(regexp_replace(CONCAT_WS(',',ds,case when ds[0] is null then \"0\" end,case when ds[1] is null then \"0\" end,case when ds[2] is null then \"0\" end,\"0,0,0\"),'1_|2_|3_',''),',','|') urllist from( 
select userid,sort_array(collect_set(CONCAT_WS('_',CONCAT_WS('_',cast(rno as String),class_name),cast(frequency as String)))) ds from( 
select t.userid,g.class_name,t.frequency,row_number() over(partition by userid,substring(lasthour,9,2) order by frequency desc) as rno from aotain_dim.to_ref_domainclass g  join tw_static_wapuseracc t  on attvalue = class_id where  partdate >= '${OP_SDAY}' and partdate <= '${OP_EDAY}' and attcode='URLNo' and substring(lasthour,9,2) = '$1') ta where rno <4 group by userid ) r1;"

    fecho "Table ${OP_TABLE_NAME} build completed"

    return 0
} 

######################
#    main procedure
######################
main "$@"
exit $?

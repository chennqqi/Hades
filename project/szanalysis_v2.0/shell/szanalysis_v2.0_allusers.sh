#!/bin/sh
# ===================================================================================  
# Filename:    szanalysis_v2.0_allusers.sh
# Revision:    2.0
# Date:        2015/0/06
# Author:      zhengsh
# Description: Analysis of demand in Shenzhen
# Notes:       szanalysis_v2.0_allusers.sh
# ===================================================================================

AOTAIN_SZUSERANALYSIS_LOG=/home/bsmp/work/log/szuseranalysis.log

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
    if [ $# -ne 1 ]; then
        echo "Invalid cmd format, correct format is as following:"
        echo "eg, ./szanalysis_v2.0_ctnetusers.sh day"
        return 1;
    fi

    # define parameters
    OP_TABLE_NAME=tmp_day_users
    OP_EDAY=`date +"%Y%m%d" --date="-1 day"`
    if [ "$1" = "month" ]; then
        premonth=`date -d last-month +%Y%m`
        OP_SDAY=${premonth}''01
        OP_EDAY=getlastday ${premonth}
    elif [ "$1" = "week" ]; then
        OP_SDAY=`date +"%Y%m%d" --date="-7 day"`
    else
        OP_SDAY=`date +"%Y%m%d" --date="-1 day"`
    fi

    fecho "Gets the ${OP_DHOUR} execution log of getuser.sh"

    # ready to create table and read data
    /usr/bin/hive -S -e "use aotain_dw; drop table if exists ${OP_TABLE_NAME};"
    /usr/bin/hive -S -e "use aotain_dw; create table ${OP_TABLE_NAME} as  select distinct userid from tw_static_useracc where partdate >= '${OP_SDAY}' and partdate <= '${OP_EDAY}';"

    fecho "Table ${OP_TABLE_NAME} build completed"

    return 0
} 

######################
#    main procedure
######################
main "$@"
exit $?

#!/bin/sh
# ===================================================================================  
# Filename:    szanalysis_v2.0_exportctwapreport.sh day
# Revision:    2.0
# Date:        2015/0/06
# Author:      zhengsh
# Description: Analysis of demand in Shenzhen
# Notes:       szanalysis_v2.0_exportctwapreport.sh day 
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
    echo "Validate parameters..."
    # validate parameters
    if [ $# -ne 1 ]; then
        echo "Invalid cmd format, correct format is as following:"
        echo "eg, ./szanalysis_v2.0_exportctwapreport.sh day "
        return 1;
    fi
    fecho  "Export diary report start"
    # define parameters
    OP_EDAY=`date +"%Y%m%d" --date="-6 day"`

    if [ "$1" = "month" ]; then
        MONTHNUM=`expr substr '${OP_EDAY}' 1 6`
        FILENAME="CTWAPWebPageVisits_${OP_EDAY}.txt"
    elif [ "$1" = "week" ]; then
        FILENAME="CTWAPWebPageVisits_Week${OP_EDAY}.txt"
    else
        FILENAME="CTWAPWebPageVisits_${OP_EDAY}.txt"
    fi 
    rm -rf ${OUTPUTPATH}/${FILENAME}

    /usr/bin/hive -S -e "use aotain_dw; select ${OP_EDAY},a.userid,b0.urllist,b1.urllist,b2.urllist,b3.urllist,b4.urllist,b5.urllist,b6.urllist,b7.urllist,b8.urllist,b9.urllist,b10.urllist,b11.urllist,b12.urllist,b13.urllist,b14.urllist,b15.urllist,b16.urllist,b17.urllist,b18.urllist,b19.urllist,b20.urllist,b21.urllist,b22.urllist,b23.urllist from tmp_day_ctwapusers a 
    left join tmp_hour_ctwapvisits_00 b0 on a.userid=b0.userid 
    left join tmp_hour_ctwapvisits_01 b1 on a.userid=b1.userid 
    left join tmp_hour_ctwapvisits_02 b2 on a.userid=b2.userid 
    left join tmp_hour_ctwapvisits_03 b3 on a.userid=b3.userid 
    left join tmp_hour_ctwapvisits_04 b4 on a.userid=b4.userid 
    left join tmp_hour_ctwapvisits_05 b5 on a.userid=b5.userid 
    left join tmp_hour_ctwapvisits_06 b6 on a.userid=b6.userid 
    left join tmp_hour_ctwapvisits_07 b7 on a.userid=b7.userid 
    left join tmp_hour_ctwapvisits_08 b8 on a.userid=b8.userid 
    left join tmp_hour_ctwapvisits_09 b9 on a.userid=b9.userid 
    left join tmp_hour_ctwapvisits_10 b10 on a.userid=b10.userid 
    left join tmp_hour_ctwapvisits_11 b11 on a.userid=b11.userid 
    left join tmp_hour_ctwapvisits_12 b12 on a.userid=b12.userid 
    left join tmp_hour_ctwapvisits_13 b13 on a.userid=b13.userid 
    left join tmp_hour_ctwapvisits_14 b14 on a.userid=b14.userid 
    left join tmp_hour_ctwapvisits_15 b15 on a.userid=b15.userid 
    left join tmp_hour_ctwapvisits_16 b16 on a.userid=b16.userid 
    left join tmp_hour_ctwapvisits_17 b17 on a.userid=b17.userid 
    left join tmp_hour_ctwapvisits_18 b18 on a.userid=b18.userid 
    left join tmp_hour_ctwapvisits_19 b19 on a.userid=b19.userid 
    left join tmp_hour_ctwapvisits_20 b20 on a.userid=b20.userid 
    left join tmp_hour_ctwapvisits_21 b21 on a.userid=b21.userid 
    left join tmp_hour_ctwapvisits_22 b22 on a.userid=b22.userid 
    left join tmp_hour_ctwapvisits_23 b23 on a.userid=b23.userid;"|sed "s/NULL/0|0|0|0|0|0/g"|sed "s/\t/|/g">${OUTPUTPATH}/${FILENAME}".WRITING"

    fecho  "Export diary report completion:${FILENAME}"
    mv ${OUTPUTPATH}/${FILENAME}".WRITING" ${OUTPUTPATH}/${FILENAME}
    #scp ${OUTPUTPATH}/${FILENAME} root@172.16.1.38:/home/nebula/outdata/szanalysis_v2.0
    #mv ${OUTPUTPATH}/${FILENAME} /home/nebula/outdata/bak/szanalysis_v2.0/${FILENAME}

    #/usr/bin/hive -S -e "use aotain_dw; drop table if exists tmp_day_ctwapusers; drop table if exists tmp_hour_ctwapvisits_00;drop table if exists tmp_hour_ctwapvisits_01;drop table if exists tmp_hour_ctwapvisits_02;drop table if exists tmp_hour_ctwapvisits_03;drop table if exists tmp_hour_ctwapvisits_04;drop table if exists tmp_hour_ctwapvisits_05;drop table if exists tmp_hour_ctwapvisits_06;drop table if exists tmp_hour_ctwapvisits_07;drop table if exists tmp_hour_ctwapvisits_08;drop table if exists tmp_hour_ctwapvisits_09;drop table if exists tmp_hour_ctwapvisits_10;drop table if exists tmp_hour_ctwapvisits_11;drop table if exists tmp_hour_ctwapvisits_12;drop table if exists tmp_hour_ctwapvisits_13;drop table if exists tmp_hour_ctwapvisits_14;drop table if exists tmp_hour_ctwapvisits_15;drop table if exists tmp_hour_ctwapvisits_16;drop table if exists tmp_hour_ctwapvisits_17;drop table if exists tmp_hour_ctwapvisits_18;drop table if exists tmp_hour_ctwapvisits_19;drop table if exists tmp_hour_ctwapvisits_20;drop table if exists tmp_hour_ctwapvisits_21;drop table if exists tmp_hour_ctwapvisits_22;drop table if exists tmp_hour_ctwapvisits_23;"

    return 0
} 

######################
#    main procedure
######################
main "$@"
exit $?


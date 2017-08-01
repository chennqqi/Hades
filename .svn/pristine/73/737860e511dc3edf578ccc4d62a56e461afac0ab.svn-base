#!/bin/bash

#Created by wayne 2014-12-14
#load phone spider data into hdfs
#Version 2.0  modified by wayne 2014-12-14

if [ $# -ne 1 ]; then
        exit 1
fi

DATE=$1

if [ "$DATE" = "-1" ]; then
        DATE=`date +%Y%m%d`
fi

TABLENAME=to_ref_phoneinfo
MRCLASS=com.aotain.dim.TerminalAttDriver
INPUTPATH=/user/hive/warehouse/temp.db/spider_t
OUTPUTPATH=/user/hive/warehouse/aotain_dim.db/$TABLENAME
CONFIG=/home/bigdata/project/dim/config/$TABLENAME.xml

MRPATH=/home/bigdata/project/dim/mr
LOGFILE=/home/bigdata/project/dim/log/$TABLENAME.log

#mapreduce
hadoop jar $MRPATH/dim.jar $MRCLASS $INPUTPATH $OUTPUTPATH $CONFIG $DATE

exit 0

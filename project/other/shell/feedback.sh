#!/bin/bash

WORKPATH=/home/bsmp/work
JAVALIB=${WORKPATH}/mr/
LOGFILE=${WORKPATH}/log/HBase_HTTP_${TABLENAME}_${CITYNAME}_${DATE}.log
CONFIGPATH=${WORKPATH}/config/


export HBASELIB=/opt/cloudera/parcels/CDH/lib/hbase
export HADOOP_CLASSPATH=$HBASELIB/hbase-client.jar:$HBASELIB/hbase-common.jar:$HBASELIB/hbase-hadoop2-compat.jar:$HBASELIB/hbase-hadoop-compat.jar:$HBASELIB/hbase-it.jar:$HBASELIB/hbase-prefix-tree.jar:$HBASELIB/hbase-protocol.jar:$HBASELIB/hbase-server.jar:$HBASELIB/hbase-shell-0.96.1.1-cdh5.0.3.jar:$HBASELIB/hbase-thrift-0.96.1.1-cdh5.0.3.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/htrace-core.jar:/opt/cloudera/parcels/CDH/lib/hbase/lib/high-scale-lib-1.1.1.jar

hadoop jar $JAVALIB/feedback.jar cheng.FeedbackIntoHbase

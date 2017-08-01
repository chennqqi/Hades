package com.aotain.hbase.dataimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HFileLoader {
	
	 public static void main(String[] args) throws Exception {
		 Configuration conf = HBaseConfiguration.create();
		 conf.set("hbase.zookeeper.quorum", "hive-2");
	        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(
	        		conf);
	        loader.doBulkLoad(new Path("hdfs://192.168.5.64:8020/user/data/output/20141203/"), new HTable(
    				conf, "uaphone".getBytes()));
	    }
	 
}

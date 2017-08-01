package com.aotain.dw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestDriver extends Configured implements Tool{
	public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
		//hbase
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hive-2");  //千万别忘记配置
        conf.set("hbase.zookeeper.property.clientPort","2181");
        
        //设置参数
        //解析的列配置，按列顺序
		conf.set("file.columns", "areaid,userid,os,sysversion,terminaltype,terminalversion,reporttime");  
	    conf.set("rowkey.index","1,6");
	    
	    @SuppressWarnings("deprecation")
        Job job = Job.getInstance(conf, "Txt-to-Hbase");
        job.setJarByClass(TestDriver.class);
	    //turk.rowkey
	    //conf.set("turk.rowkeys","BSC:ACCESS_CELL");
	    //turk.column
	    //conf.set("turk.column","CALL_DURATION");
	    //turk.outcolumn
	    //conf.set("turk.outcolumn",columnname);
        
        //需要分析的文件目录
        Path in = new Path(arg0[0]);
        
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, in);
        
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        TableMapReduceUtil.initTableReducerJob("dwuser", TestReducer.class, job);
        
        job.waitForCompletion(true);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new TestDriver(),args);
        System.exit(mr);
    }
}

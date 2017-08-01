package com.aotain.hbase.dataimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsToHbaseImport extends Configured implements Tool{

	public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
		
		 Configuration conf = HBaseConfiguration.create();
		 conf.set("hbase.zookeeper.quorum", "172.16.1.40");
		 
		/**解析参数
		 * [0] 默认输入分析的文件目录
		 * columns=aaa,bbb,ccc,ddd
		 * rowkey=aaa,bbb  rowkey必须是在columns中出现的
		 */
		String tableName = "";
		for(String arg:arg0)
		{
			String value = "";
			if(arg.contains("columns"))
			{//Hbase.columns
				value = arg.split("=",-1)[1];
				conf.set("Hbase.columns",value);
			}
			if(arg.contains("rowkey"))
			{
				value = arg.split("=",-1)[1];
				conf.set("Hbase.rowkey",value);
			}
			if(arg.contains("table"))
			{//Hbase.columns
				value = arg.split("=",-1)[1];
				tableName = value;
			}
			if(arg.contains("host"))
			{//Hbase.columns
				value = arg.split("=",-1)[1];
				conf.set("hbase.zookeeper.quorum", value);
			}
		}
		
		  //千万别忘记配置
	    conf.set("hbase.zookeeper.property.clientPort","2181");      
		
		System.out.println("Hbase.columns="+conf.get("Hbase.columns"));
		System.out.println("rowkey="+conf.get("Hbase.rowkey"));
		
		//将rowkey的字段转换成对应的列index
		String sKey = conf.get("Hbase.rowkey");
		String rowkey = "";
		for(String key:sKey.split(",",-1))
		{
			String[] columns = conf.get("Hbase.columns").split(",",-1);
			for(int i = 0;i < columns.length; i++)
			{
				if(key.equals(columns[i]))
				{
					rowkey = rowkey + i + ",";
					break;
				}
			}
		}
		
		rowkey = rowkey.substring(0,rowkey.length() - 1);
		conf.set("Hbase.rowkey",rowkey);
		System.out.println("Hbase.rowkey="+conf.get("Hbase.rowkey"));
		
		
		
		//hbase
       
        //conf.set("hbase.zookeeper.quorum", "hive-2");  //千万别忘记配置
        //conf.set("hbase.zookeeper.property.clientPort","2181");        
	    
	    @SuppressWarnings("deprecation")
        Job job = Job.getInstance(conf, "Hbase Import");
        job.setJarByClass(this.getClass());
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
        
        job.setMapperClass(HdfsToHbaseMapper.class);
        job.setReducerClass(HdfsToHbaseReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        //job.setOutputKeyClass(ImmutableBytesWritable.class);
        //job.setOutputValueClass(Put.class);   
        job.setNumReduceTasks(100);
        //TableMapReduceUtil.setNumReduceTasks(table, job);
        TableMapReduceUtil.initTableReducerJob(tableName, HdfsToHbaseReducer.class, job);
        
        job.waitForCompletion(true);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new HdfsToHbaseImport(),args);
        System.exit(mr);
    }
}

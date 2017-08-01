package com.aotain.hbase.dataimport;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.common.HfileConfig;

@SuppressWarnings("deprecation")
public class HFileKVData extends Configured implements Tool{
	@SuppressWarnings("deprecation")
	public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
		
		Configuration conf = new Configuration(); 
		
		if(arg0.length!=4)
			return 1;
		String inputPath = arg0[0];
		String targetPath = arg0[1];
		String Config = arg0[2];
		String time = arg0[3];
		
		HFileConfigMgr configMgr = null;
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        
        configMgr = new HFileConfigMgr(Config);
		HfileConfig confHfile = configMgr.config;
		String tableName = confHfile.getTableName();
		
		String kvsplit = confHfile.getKVSplit();
		conf.set("Hbase.kvsplit",kvsplit);
		System.out.println("-------------kvsplit: " + kvsplit);
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("Hbase.fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);
		
		String rowkey = "";
		
		if(confHfile.getRowKey().size()<1 && kvsplit.length()<1 && fieldsplit.length()<1)
		{
			System.out.println("-------------configfile error!");
			return 1;
		}
			
		for(FieldItem item : confHfile.getRowKey())
		{
			String text = String.format("%s", item.FieldName);
			rowkey = rowkey + text +","; 
		}
		rowkey  = rowkey.substring(0,rowkey.length() -1 );
		conf.set("Hbase.rowkey",rowkey);
		System.out.println("-------------rowkey: " + rowkey);
		
        Job job = Job.getInstance(conf);
        job.setJobName("HFile output[" + time + "], TableName:"+ tableName);      
        
        job.setJarByClass(HFileKVData.class);
 
        job.setMapperClass(HFileKVDataMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
 
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
 
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(targetPath));
        HFileOutputFormat2.configureIncrementalLoad(job,
        		new HTable(conf, tableName));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new HFileKVData(),args);
        System.exit(mr);
    }
}

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
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.common.HbaseCommon;
import com.aotain.common.HfileConfig;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * Hbase 数据导入方法，通过MR形成HFILE在通过bulkload导入到hbase中
 * @author Administrator
 *
 */
@SuppressWarnings("deprecation")
public class HFileOutput extends Configured implements Tool{
	@SuppressWarnings("deprecation")
	public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
		
		
		Configuration conf = new Configuration(); 
		String targetPath = "";
		String inputPath = "";
		HFileConfigMgr configMgr = null;
		/**解析参数
		 * [0] 默认输入分析的文件目录
		 * columns=aaa,bbb,ccc,ddd
		 * rowkey=aaa,bbb  rowkey必须是在columns中出现的
		 */
		String tableName = "";
		String time = "";
		String Config = "";
		for(String arg:arg0)
		{
			String value = "";
			if(arg.contains("config"))
			{//Hbase.columns 通过读取xml配置文件获取到入库的配置信息
				value = arg.split("=",-1)[1];
				Config = value;//配置文件名称 xxx.xml
				
				configMgr = new HFileConfigMgr(Config);
				HfileConfig confHfile = configMgr.config;
				//tableName = confHfile.getTableName();
				//inputPath = confHfile.getInput();
				//targetPath = confHfile.getOutput();
				
				String columns = "";
				for(FieldItem item : confHfile.getColumns())
				{
					columns = columns + item.FieldName + ","; 
				}
				columns  = columns.substring(0,columns.length() -1 );
				conf.set("Hbase.columns",columns);
				
				String rowkey = "";
				for(FieldItem item : confHfile.getRowKey())
				{
					String text = String.format("%s|%d", item.ColumnIndex,item.DataLength);
					rowkey = rowkey + text + ","; 
				}
				rowkey  = rowkey.substring(0,rowkey.length() -1 );
				conf.set("Hbase.rowkey",rowkey);
			}
			
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
			if(arg.contains("time"))
			{//Hbase.columns
				value = arg.split("=",-1)[1];
				time = value;
			}
			if(arg.contains("partition"))
			{//Hbase.columns
				value = arg.split("=",-1)[1];
				//value = value.substring(0,6);//一个月一个分区
				//tableName = tableName+"_" + value;
				tableName = tableName.toUpperCase();
				HbaseCommon.createTable(tableName.toUpperCase());
			}
			
		}
		
		//if(configMgr == null)
		{
			inputPath = arg0[0];
			targetPath = arg0[1];
			//将rowkey的字段转换成对应的列index
			
		}
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
      	  	System.out.println("Delete path " + targetPath);
        }
        
		
		System.out.println("Hbase.columns="+conf.get("Hbase.columns"));
		System.out.println("rowkey="+conf.get("Hbase.rowkey"));
	
		
        Job job = Job.getInstance(conf);
        job.setJobName("HFile output[" + time + "], TableName:"+ tableName);      
       
        //job.setOutputFormatClass(LzoTextInputFormat.class);
        job.setInputFormatClass(LzoTextInputFormat.class);
        job.setJarByClass(HFileOutput.class);
 
        job.setMapperClass(HFileOutputMapper.class);
        //job.setReducerClass(KeyValueSortReducer.class);
        job.setReducerClass(HFileOutputReducer.class);
        
  
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
 
 
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(targetPath));
        //HFileOutputFormat.setCompressOutput(job,false);
        //FileOutputFormat.setCompressOutput(job, false);
        //HFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        HTable htable = new HTable(
				conf, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job,
        		htable);
        job.waitForCompletion(true);
        
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        //-----  执行BulkLoad  -------------------------------------------------------------------------------
        //HdfsUtil.chmod(conf, output.toString());
        //HdfsUtil.chmod(conf, output + "/" + YeepayConstant.COMMON_FAMILY);
        //htable = new HTable(conf, tableName);
        new LoadIncrementalHFiles(conf).doBulkLoad(pathTarget, htable);
        System.out.println("HFile data load success!");
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
      	  	System.out.println("Delete path " + targetPath);
        }
        FileSystem fsInput = FileSystem.get(URI.create(inputPath),conf);
        Path pathInput = new Path(inputPath);
        if(fsInput.exists(pathInput))
        {
      	  	fsTarget.delete(pathInput, true);
      	  	System.out.println("Delete path " + inputPath);
        }
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new HFileOutput(),args);
        System.exit(mr);
    }
}

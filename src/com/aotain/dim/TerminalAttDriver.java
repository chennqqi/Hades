package com.aotain.dim;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;

public class TerminalAttDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=4)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String Config = args[2];
		String date = args[3];
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
		HFileConfigMgr configMgr = null;
        
        configMgr = new HFileConfigMgr(Config);
		HfileConfig confHfile = configMgr.config;
		String tableName = confHfile.getTableName();
		
		conf.set("date",date);
		
		String kvsplit = confHfile.getKVSplit()==null?"null":confHfile.getKVSplit();
		conf.set("kvsplit",kvsplit);
		System.out.println("-------------kvsplit: " + kvsplit);
		
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);
		
		String rowkey = "";	
		for(FieldItem item : confHfile.getRowKey())
		{
			String text = String.format("%s=%s", item.FieldName,item.RegExp.length()>0?item.RegExp:"null");
			rowkey += text +"#"; 
		}
		rowkey  = rowkey.substring(0,rowkey.length() -1 );
		conf.set("rowkey",rowkey);
		System.out.println("-------------rowkey: " + rowkey);
		
		String column = "";
		for(FieldItem item : confHfile.getColumns())
		{
			String text = String.format("%s=%s", item.FieldName,item.RegExp.length()>0?item.RegExp:"null");
			column += text +"#"; 
		}
		column  = column.substring(0,column.length() -1 );
		conf.set("column",column);
		System.out.println("-------------column: " + column);
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);
        job.setMapperClass(TerminalAttMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(TerminalAttReducer.class);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new TerminalAttDriver(), args);
         System.exit(exitcode);                  
    }   
}
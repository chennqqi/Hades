package com.aotain.project.sada;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.hadoop.mapreduce.LzoTextInputFormat;

import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;

public class UserAttDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=4)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String Config = args[2];
		String date = args[3];
		

		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
		HFileConfigMgr configMgr = null;
        
        configMgr = new HFileConfigMgr(Config);
		HfileConfig confHfile = configMgr.config;
		
		String tn = confHfile.getTableName();
		String[] splits = tn.split(",",-1);
		
		String tableName = splits[0];
		String limitday = splits.length>1?splits[1]:"null";
		conf.set("limitday",limitday);
		System.out.println("-------------limitday: " + limitday);
		
		conf.set("date",date);
		
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);
		
		String rowkey = "";
		for (FieldItem item : confHfile.getRowKey()) {
			rowkey = String.format("%d=%s=%s", item.FieldIndex, item.FieldName,
					item.RegExp.length() > 0 ? item.RegExp : "null");
			break;
		}
		conf.set("rowkey", rowkey);
		System.out.println("-------------rowkey: " + rowkey);

		String column = "";
		for (FieldItem item : confHfile.getColumns()) {
			String text = String.format("%d=%s=%s", item.FieldIndex,
					item.FieldName, item.RegExp.length() > 0 ? item.RegExp
							: "null");
			column += text + "#";
		}
		column = column.substring(0, column.length() - 1);
		conf.set("column", column);
		System.out.println("-------------column: " + column);

		String filter = "";
		for (FieldItem item : confHfile.getFilter()) {
			String text = String.format("%s=%s", item.FieldIndex,
					item.RegExp.length() > 0 ? item.RegExp : "null");
			filter += text + "#";
		}
		if (filter.length() > 0) {
			filter = filter.substring(0, filter.length() - 1);
		} else {
			filter = "null";
		}
		conf.set("filter", filter);
		System.out.println("-------------filter: " + filter);

        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(UserAttReducer.class);
        job.setMapOutputValueClass(Text.class);    

        String[] paths = inputPath.split("#");
        for(int i=0; i<paths.length; i++)
        {
        	String[] p = paths[i].split("\\|");
        	if(p[1].equals("lzo"))
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),LzoTextInputFormat.class,UserAttMapper.class);
            	System.out.println("-------------lzopaths: "+p[0]);
        	}
        	else if(p[1].equals("orc"))
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),OrcNewInputFormat.class,UserAttOrcMapper.class);
            	System.out.println("-------------orcpaths: "+p[0]);
        	}
        	else
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),TextInputFormat.class,UserAttMapper.class);
            	System.out.println("-------------txtpaths: "+p[0]);
        	}
        }
        
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new UserAttDriver(), args);
         System.exit(exitcode);                  
    }   
}
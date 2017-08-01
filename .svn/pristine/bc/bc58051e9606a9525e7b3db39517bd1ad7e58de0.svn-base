package com.aotain.project.npcheck;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.ObjectSerializer;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class NPointCheckDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  

		if(args.length!=4)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String[] inputPaths = args[0].split(",");
		String targetPath = args[1];
		String Config = args[2];
		String date = args[3];
		
		//������Ŀ¼�Ѵ��ڣ���Ҫɾ��
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
		
	    conf.set("date", date);
		
		String fieldsplit = confHfile.getFieldSplit();
		conf.set("fieldsplit",fieldsplit);
		System.out.println("-------------fieldsplit: " + fieldsplit);
		
		String column = "";
		Map<String,String>webconfigmap=new HashMap<String, String>();
		for(FieldItem item : confHfile.getColumns())
		{
			String text = String.format("%s=%s=%s", item.FieldName,item.RegExp.length()>0?item.RegExp:"null",item.FieldIndex);
			//column += text +"#";  
			webconfigmap.put(item.FieldName, text);
		}
		//column  = column.substring(0,column.length() -1 );
		conf.set("column", ObjectSerializer.serialize((Serializable) webconfigmap));
		
		//System.out.println("-------------column: " + column);
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapperClass(NPointCheckMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(NPointCheckReducer.class);
        //job.setNumReduceTasks(10);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(OrcNewInputFormat.class);
        //job.setInputFormatClass(LzoTextInputFormat.class);
        for(int i=0;i<inputPaths.length;i++){
        	System.out.println(inputPaths[i]);
        FileInputFormat.addInputPath(job,new Path(inputPaths[i]));
        }
       
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new NPointCheckDriver(), args);
         System.exit(exitcode);                  
    }   
}
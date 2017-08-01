package com.aotain.project.szreport;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HfileConfig;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class CountqqDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {                  

		if(args.length!=4)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String[] inputPaths = args[0].split(",");
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
	    String tableName = "qqcount";
	    conf.set("date", date);
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapperClass(CountqqMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(CountqqReducer.class);
        //job.setNumReduceTasks(10);
        job.setMapOutputValueClass(Text.class);   
        MultipleOutputs.addNamedOutput(job,"qqnum",TextOutputFormat.class,Text.class,IntWritable.class);  
        MultipleOutputs.addNamedOutput(job,"qqstatistics",TextOutputFormat.class,Text.class,IntWritable.class); 
        //job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(LzoTextInputFormat.class);
        for(int i=0;i<inputPaths.length;i++){
        	System.out.println(inputPaths[i]);
        FileInputFormat.addInputPath(job,new Path(inputPaths[i]));
        }
       
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new CountqqDriver(), args);
         System.exit(exitcode);                  
    }   
}

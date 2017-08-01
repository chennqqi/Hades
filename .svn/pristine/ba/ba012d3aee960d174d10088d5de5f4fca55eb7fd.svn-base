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

public class AreaUserParseDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=3)
			return 1;
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String date = args[2];
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName: AreaUserParse");                    
        job.setJarByClass(getClass());
        job.setMapperClass(AreaUserParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(AreaUserParseReducer.class);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(20);
        FileInputFormat.addInputPath(job,new Path(inputPath)); 
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new AreaUserParseDriver(), args);
         System.exit(exitcode);                  
    }   
}
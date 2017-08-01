package com.aotain.dw;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * 记录用户一次行为的最后一条URL
 * @author Administrator
 *
 */
public class UserLastUrlDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length != 2){
             	System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());
             	ToolRunner.printGenericCommandUsage(System.err);
             	return -1;                  
		    }                  
		    
		    Configuration conf =getConf();   
		    //目标目录维护
		    String targetPath = args[1];
		    System.out.println("targetPath: " + targetPath);
		    FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
		    Path pathTarget = new Path(targetPath);
		    if(fsTarget.exists(pathTarget))
		    {
		    	fsTarget.delete(pathTarget, true);
		    	System.out.println("Delete path " + targetPath);
		    }
		    	
		 
		    Job job = Job.getInstance(conf);
		    
		    job.setJobName("User Last URL");                  
		
		    job.setJarByClass(getClass());
		
		    FileInputFormat.addInputPath(job,new Path(args[0]));
		    
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    
		    job.setInputFormatClass(LzoTextInputFormat.class);
		    FileOutputFormat.setCompressOutput(job, true);
	        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	        
		    job.setMapperClass(UserLastUrlMapper.class);
		    job.setCombinerClass(UserLastUrlCombiner.class);
		    job.setReducerClass(UserLastUrlReducer.class);   
		    job.setOutputKeyClass(Text.class);
		    job.setNumReduceTasks(256);
		    job.setOutputValueClass(Text.class);  
		    
		    return job.waitForCompletion(true)?0:1;   
	}

	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new UserLastUrlDriver(),args);
        System.exit(mr);
    }
}

package com.aotain.ods;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MailFileDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=3)
		{
			System.out.print("args error!");
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPostPath = args[0];
		String targetPath = args[1];
		String date = args[2];
		
		FileSystem fsSource = FileSystem.get(URI.create(inputPostPath), conf);
		Path pathSource = new Path(inputPostPath);
		if(!fsSource.exists(pathSource)) {
			return 0;
		}
	
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        conf.set("date",date);
        Job job = Job.getInstance(conf);
        job.setJobName("PostData File[" + date + "]");                    
        job.setJarByClass(getClass());
        
        job.setMapperClass(MailFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(120);
        job.setReducerClass(MailFileReducer.class);
        MultipleOutputs.addNamedOutput(job,"Mail",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"IMEI",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"MAC",TextOutputFormat.class,Text.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"IDFA",TextOutputFormat.class,Text.class,Text.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
        
        FileInputFormat.addInputPath(job,new Path(inputPostPath));

        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new MailFileDriver(), args);
        System.exit(exitcode);                  
   }   
}

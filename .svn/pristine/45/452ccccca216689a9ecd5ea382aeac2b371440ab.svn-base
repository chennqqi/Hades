package com.aotain.project.sada;

import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class UserIdentifierSDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=3)
		{
			System.out.print("args error!");
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String[] inputPaths = args[0].split("#");
		String targetPath = args[1];
		String date = args[2];
	
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
        conf.set("date",date);
        Job job = Job.getInstance(conf);
        job.setJobName("User Identifiers File[" + date + "]");                    
        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(UserIdentifierSReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
        
	    for(int i = 0; i < inputPaths.length; i ++)
        {
        	MultipleInputs.addInputPath(job,new Path(inputPaths[i]),TextInputFormat.class,UserIdentifierSMapper.class);
        }

        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new UserIdentifierSDriver(), args);
        System.exit(exitcode);                  
   }   
}

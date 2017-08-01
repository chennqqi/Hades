package com.aotain.project.cnshuidi;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class PingAnDriver 
	extends Configured implements Tool {
	
	private StringBuffer urlList = new StringBuffer();
	
	    public int run(String[] args) throws Exception {                  
      /*0:source
       *1：target
       *2：conf
       * */
	              if (args.length != 3){

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
	              	
	              	
	              String confuri = args[2];
	              FileSystem fs = FileSystem.get(URI.create(confuri),conf); 
	              FSDataInputStream in = null; 
	              try{ 
	              	
	              	Path path = new Path(confuri);
	              	if(fs.exists(path))
	              	{
	              		System.out.println(confuri); 
	              		System.out.println("exist file !!!!!!"); 
	              	  try{
	              	  in = fs.open(new Path(confuri));
	              	  BufferedReader bis = new BufferedReader(new InputStreamReader(in,"GBK")); 
	              	  String line = "";
	              	  while ((line = bis.readLine()) != null) {  
		      		  
	              		urlList.append(line + "|");
		      		  }
	              	  urlList.deleteCharAt(urlList.length()-1);//去掉最后一个逗号
		              System.out.println(urlList.toString()); 
		             
	              	  }finally{
	              	  IOUtils.closeStream(in);
	              	  }
	              	}
	              	else
	              	{
	              		System.out.println(confuri); 
	              		System.out.println("not exist file !!!!!!"); 
	              	}
	              }finally{ 
	                  IOUtils.closeStream(in); 
	              } 

	              
	              conf.set("app.urllist", urlList.toString());  
	              Job job = Job.getInstance(conf);
	              
	              job.setJobName("PingAn URL");                  

	              job.setJarByClass(getClass());

	              FileInputFormat.addInputPath(job,new Path(args[0]));

	              FileOutputFormat.setOutputPath(job,new Path(args[1]));
	              
	              job.setInputFormatClass(LzoTextInputFormat.class);
	              
	              job.setNumReduceTasks(1);

	              job.setMapperClass(PingAnMapper.class);

	              job.setReducerClass(PingAnReducer.class);                  

	              job.setOutputKeyClass(Text.class);

	              job.setOutputValueClass(Text.class);                  

	              return job.waitForCompletion(true)?0:1;                  

	    }


	    public static void main(String[] args)throws Exception{
	         int exitcode = ToolRunner.run(new PingAnDriver(), args);
	         System.exit(exitcode);                  
	    }   
}

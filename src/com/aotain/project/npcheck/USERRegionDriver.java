package com.aotain.project.npcheck;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class USERRegionDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length != 3)
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
		conf.set("date",date);
        Job job = Job.getInstance(conf);
        job.setJobName("last check region [" + date + "]");                    
        job.setJarByClass(getClass());
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(USERRegionR.class);
        job.setMapOutputKeyClass(Text.class);
        
        String[] paths = inputPath.split("#");
        for(int i=0; i<paths.length; i++)
        {
        	MultipleInputs.addInputPath(job,new Path(paths[i]),TextInputFormat.class,USERRegionM.class);
        }
        FileOutputFormat.setOutputPath(job,new Path(targetPath));    

        
        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new USERRegionDriver(), args);
         System.exit(exitcode);                  
    }   
}


class USERRegionM extends Mapper<LongWritable,Text,Text,Text>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{	  
		  if(value.toString().contains(",")){
			  String[] items = value.toString().split(",",-1);
			  context.write(new Text(items[0].trim()),new Text(items[1]+","+"1"));
		  }else{
			  String[] items = value.toString().split("\\001",-1);
			  if(items[2] != null && items[2].length() > 0 && !items[2].equals("\\N")){
				  context.write(new Text(items[0].trim()),new Text(items[2]+","+"2"));
			  }
		  }
	  }
}

class USERRegionR extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
		 	try {
		 		
				     String vkey= key.toString().trim();
				     Text v = new Text();
				     String newrg=new String();
				     Map<String,String> map=new HashMap<String,String>();
			         for(Text value: values){
		        		 String[] items = value.toString().split(",");
		        		 map.put(items[1], items[0]);
			         }
			        
			         if(map.get("2") != null){
			        	 newrg=map.get("2");
			         }else{
			        	 newrg=map.get("1");
			         }
			         
			         v.set(String.format("%s,%s,",vkey,newrg));
			         context.write(v, new Text("")); 
			} catch (Exception e) {
				// TODO: handle exception
			}
     } 
}
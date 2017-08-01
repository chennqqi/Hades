package com.aotain.project.dmp;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class AnalysisDataLabelDriver extends Configured implements Tool {
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
	    String tableName = "datalabel";
	    conf.set("date", date);
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [" + date + "], TableName:"+ tableName);                    
        job.setJarByClass(getClass());
        job.setMapperClass(AnalysisDataLabelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(AnalysisDataLabelReducer.class);
        job.setNumReduceTasks(120);
        job.setMapOutputValueClass(Text.class);  
        //job.setOutputFormatClass(OrcNewOutputFormat.class);
        MultipleOutputs.addNamedOutput(job,"newHouse",TextOutputFormat.class,Text.class,IntWritable.class);  
        MultipleOutputs.addNamedOutput(job,"keyword",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"industry",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"otherHouse",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"type",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"newCar",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"specialbv",TextOutputFormat.class,Text.class,IntWritable.class); 
        MultipleOutputs.addNamedOutput(job,"specialbvct",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"carindustry",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"lablefreq",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"webkey",TextOutputFormat.class,Text.class,IntWritable.class);
        MultipleOutputs.addNamedOutput(job,"webkeyLabel",TextOutputFormat.class,Text.class,IntWritable.class);
      // job.setInputFormatClass(TextInputFormat.class);
       job.setInputFormatClass(OrcNewInputFormat.class);
        for(int i=0;i<inputPaths.length;i++){
        	System.out.println(inputPaths[i]);
        FileInputFormat.addInputPath(job,new Path(inputPaths[i]));
        }
       
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new AnalysisDataLabelDriver(), args);
         System.exit(exitcode);                  
    }   
}

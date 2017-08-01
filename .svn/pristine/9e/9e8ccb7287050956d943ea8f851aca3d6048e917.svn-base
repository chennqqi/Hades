package com.aotain.dw;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

public class DomainStatDriver  extends Configured implements Tool{
	public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
		//hbase
       
	    @SuppressWarnings("deprecation")
	    Configuration conf =getConf();  
	    String sourcePath = args[0];
	    System.out.println("sourcePath: " + sourcePath);
	    FileSystem fsSource = FileSystem.get(URI.create(sourcePath),conf);
	    Path pathSource = new Path(sourcePath);
	    if(!fsSource.exists(pathSource))
	    {
	    	return 0;
	    }
	    
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
	    
	    String strRemark = args[2];
	    
	    Job job = Job.getInstance(conf);
	    job.setJobName("Domain stat [" + strRemark + "]");                  
	    job.setJarByClass(getClass());
	    FileInputFormat.addInputPath(job,new Path(args[0]));
	    
	    //FileInputFormat.setInputPathFilter(job,HTTPPathFilter.class);
	    FileOutputFormat.setOutputPath(job,new Path(args[1]));
	    
	    job.setInputFormatClass(LzoTextInputFormat.class);
	    //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
        
	    job.setMapperClass(DomainStatMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    //job.setCombinerClass(HTTPFilterCombiner.class);
	    job.setReducerClass(DomainStatReducer.class);   
	    job.setOutputKeyClass(Text.class);
	    //job.setNumReduceTasks(1);
	    job.setOutputValueClass(Text.class);  
	    
      
        job.waitForCompletion(true);
        return 0;
    }
	
	
	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new DomainStatDriver(),args);
        System.exit(mr);
    }

}

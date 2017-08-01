package com.aotain.project.other;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TmpHttpDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
        
/*0:inputfile
*1£ºtarget
*2:city config //sz¹ýÂË³öÀ´
* */
        if (args.length != 2){

                 System.err.printf("Usage: %s <input><output>",getClass().getSimpleName());

                 ToolRunner.printGenericCommandUsage(System.err);

                 return -1;                  

        }                  
        
        Configuration conf =getConf();   
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
        
        job.setJobName("TmpHttpDriver");                  

        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job,new Path(args[0]));

        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TmpHttpMapper.class);

        job.setReducerClass(TmpHttpReducer.class);                  

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);                  

        return job.waitForCompletion(true)?0:1;                  


	}
	
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new TmpHttpDriver(), args);
		System.exit(exitcode);
	}
}

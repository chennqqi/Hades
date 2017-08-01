package com.aotain.dw;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;

public class PostFormatInfoDriver extends Configured implements Tool {

	private String domains = "jd.com|taobao.com|tmall.com|chiphell.com|kaola.com|gome.com.cn|suning.com";
	
	public int run(String[] args) throws Exception {                  
		/*0:source
	     *1：target
	     *2：conf
	     * */
		if (args.length != 3){
			System.err.printf("Usage: %s <input><output><config>",getClass().getSimpleName());
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
		
		String sourcePath = args[0];
		FileSystem fsSource = FileSystem.get(URI.create(sourcePath),conf);
		Path pathSource = new Path(sourcePath);
		if(!fsSource.exists(pathSource))
		{
			System.out.println("Source path not exist!" + targetPath);
			return 0;
		}
		
		String confuri = args[2];
		HFileConfigMgr configMgr = new HFileConfigMgr(confuri);
        
		StringBuilder colums = new StringBuilder();
		for(FieldItem item:configMgr.config.getColumns())
		{
			colums.append(item.FieldName + ",");
		}
		System.out.println("columns " + colums.toString());
		conf.set("app.columns", colums.toString());
		conf.set("app.domains", domains);
		Job job = Job.getInstance(conf);
		
		job.setJobName("Post Info");            
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setNumReduceTasks(1);
		job.setMapperClass(PostFormatInfoMapper.class);
		job.setReducerClass(PostFormatInfoReducer.class);                  
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);                  
		return job.waitForCompletion(true)?0:1;                  
	}


		    public static void main(String[] args)throws Exception{
		         int exitcode = ToolRunner.run(new PostFormatInfoDriver(), args);
		         System.exit(exitcode);                  
		    }   
}

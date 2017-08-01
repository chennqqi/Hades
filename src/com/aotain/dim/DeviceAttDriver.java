package com.aotain.dim;

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

import com.aotain.common.PropertiesUtil;

public class DeviceAttDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if(args.length!=3) {
			usage("Wrong number of arguments: " + args.length);
			return 1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		String config = args[2];
		
		//如果输出目录已存在，需要删除
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
		PropertiesUtil prop = new PropertiesUtil(config);
		String keyValueSplit = prop.getString("KEY_VALUE_SPLIT");
		System.out.println("-------------KEY_VALUE_SPLIT: " + keyValueSplit);
		conf.set("KEY_VALUE_SPLIT", keyValueSplit);
		
		String fileSplit = prop.getString("FIELD_SPLIT");
		System.out.println("-------------FIELD_SPLIT: " + fileSplit);
		conf.set("FIELD_SPLIT", fileSplit);
		
		String columns = prop.getString("COLUMNS");
		System.out.println("-------------COLUMNS: " + columns);
		conf.set("COLUMNS", columns);
		
		String keyColumns = prop.getString("KEY_COLUMNS");
		System.out.println("-------------KEY_COLUMNS: " + keyColumns);
		conf.set("KEY_COLUMNS", keyColumns);
		
		String newSplit = prop.getString("NEW_SPLIT");
		System.out.println("-------------NEW_SPLIT: " + newSplit);
		conf.set("NEW_SPLIT", newSplit);
		
		String typeValue = prop.getString("TYPE_VALUE");
		System.out.println("-------------TYPE_VALUE: " + typeValue);
		conf.set("TYPE_VALUE", typeValue);
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [DeviceAttrProcessing]");                    
        job.setJarByClass(getClass());
        job.setNumReduceTasks(1);
        job.setMapperClass(DeviceAttMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(DeviceAttReducer.class);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(TextInputFormat.class);
        
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }
    
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			System.out.println("ERROR: " + errorMsg);
		}
		System.out.println("Usage: hadoop jar <jarfile> com.aotain.dim.DeviceAttDriver  <in> <out> <config_file>");

	}

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new DeviceAttDriver(), args);
         System.exit(exitcode);                  
    }   
}
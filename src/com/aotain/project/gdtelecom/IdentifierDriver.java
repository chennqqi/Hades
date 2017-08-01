package com.aotain.project.gdtelecom;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.ObjectSerializer;

/**
 * 用户标识解析，丢弃
 * 改用com.aotain.project.gdtelecom.identifier.IdentifierDriver
 * @author Administrator
 *
 */
public class IdentifierDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=5)
		{
			System.out.print("args error!");
			return -1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String[] inputPaths = args[0].split("#");
		String phonePath = args[1];
		String notnk = args[2];
		String targetPath = args[3];
		String date = args[4];
			
		Map<String,String> map=new HashMap<String,String>();
		 FileSystem fsPhone = FileSystem.get(URI.create(phonePath),conf); 
		FSDataInputStream in = null;
		try {
			Path path = new Path(phonePath);
			if (fsPhone.exists(path)) {
				for (FileStatus file : fsPhone.listStatus(path)) {
					in = fsPhone.open(file.getPath());
					BufferedReader bis = new BufferedReader(
							new InputStreamReader(in, "UTF8"));
					String line = "";
					while ((line = bis.readLine()) != null) {
						String[] arr = line.split("\\|", -1);
						map.put(arr[1], arr[1]);
					}
				}
			} else {
				System.out.println("not exist file !");
			}
		} finally {
			if (in != null)
				IOUtils.closeStream(in);
		}
		
		conf.set("map", ObjectSerializer.serialize((Serializable) map));
		conf.set("notnk", notnk);
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
	
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
        conf.set("date",date);
        Job job = Job.getInstance(conf);
        job.setJobName("Identifier File[" + date + "]");                    
        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(IdentifierReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
	    
	    for(int i=0; i< inputPaths.length; i++)
        {
        	String[] p = inputPaths[i].split("\\|");
            if(p[1].equalsIgnoreCase("get"))
        	{
            	MultipleInputs.addInputPath(job,new Path(p[0]),OrcNewInputFormat.class,IdentifierGMapper.class);
            	System.out.println("-------------getpath: "+p[0]);
        	}
        	else if(p[1].equalsIgnoreCase("post"))
        	{
        		MultipleInputs.addInputPath(job,new Path(p[0]),OrcNewInputFormat.class,IdentifierPMapper.class);
            	System.out.println("-------------postpath: "+p[0]);
        	}
        }

        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;  
	}
	
	public static void main(String[] args)throws Exception{
        int exitcode = ToolRunner.run(new IdentifierDriver(), args);
        System.exit(exitcode);                  
   }   
}

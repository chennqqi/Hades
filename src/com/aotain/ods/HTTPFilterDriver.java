package com.aotain.ods;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HTTPPathFilter;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;

/**
 * 过滤HTTP数据提炼有用数据统计使用
 * @author Administrator
 *
 */
public class HTTPFilterDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length != 4){

             System.err.printf("Usage: %s <input><output><configpath><remark>",getClass().getSimpleName());

             ToolRunner.printGenericCommandUsage(System.err);

             return -1;                  

		    }                  
		    
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
		    
		    	
		    //从黑名单中过滤掉干扰domain
		    String confuri = args[2];//"/user/hive/warehouse/aotain_dim.db/to_ref_terminalinfo/";
		    FileSystem fs = FileSystem.get(URI.create(confuri),conf); 
            FSDataInputStream in = null; 
            
            ArrayList<String> keylist = new ArrayList<String>();
            StringBuilder deviceList = new StringBuilder();
            try{ 
            	
            	Path path = new Path(confuri);
            	//ContentSummary cs = fs.getContentSummary(path);
            	//BlockLocation[] bl = fs.getFileBlockLocations(path, 0, 100);
            	
            	if(fs.exists(path))
            	{
            		System.out.println(confuri); 
            		System.out.println("exist conf file !!!!!!"); 
            		try{
            	  
            			
            			
            					//file.readFields(in);
            					in = fs.open(path);
    	            			BufferedReader bis = new BufferedReader(new InputStreamReader(in,"UTF8")); 
    	            			String line = "";
    	            			while ((line = bis.readLine()) != null) {  
    	            				keylist.add(line);
    	            			}
            			//deviceList.deleteCharAt(deviceList.length()-1);//去掉最后一个逗号
            			//System.out.println(deviceList.toString()); 
	             
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
            for(String key:keylist)
            {
            	deviceList.append(key + "|");
            }
            String strRemark = args[3];
            
            System.out.println(deviceList.toString()); 
            conf.set("app.domains", deviceList.toString());  
            conf.set("app.date", strRemark.split("_",-1)[1]);
		    Job job = Job.getInstance(conf);
		    
		    job.setJobName("HTTP Filter [" + strRemark + "]");                  
		
		    job.setJarByClass(getClass());
		
		    FileInputFormat.addInputPath(job,new Path(args[0]));
		    
		    //FileInputFormat.setInputPathFilter(job,HTTPPathFilter.class);
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    
		    job.setInputFormatClass(LzoTextInputFormat.class);
		    FileOutputFormat.setCompressOutput(job, true);
	        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
	        
		    job.setMapperClass(HTTPFilterMapper.class);
		    //job.setCombinerClass(HTTPFilterCombiner.class);
		    job.setReducerClass(HTTPFilterReducer.class);   
		    job.setOutputKeyClass(Text.class);
		    job.setNumReduceTasks(256);
		    job.setOutputValueClass(Text.class);  
		    
		    
		    int ret = job.waitForCompletion(true)?0:1;
		    LzoIndexer lzoIndexer = new LzoIndexer(conf);
	        lzoIndexer.index(new Path(args[1]));
	        
		    return ret;   
	}

	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new HTTPFilterDriver(),args);
        System.exit(mr);
    }
}

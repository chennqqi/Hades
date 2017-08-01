package com.aotain.dw;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import com.aotain.common.ObjectSerializer;
import com.hadoop.mapreduce.LzoTextInputFormat;


/**
 * 清洗终端数据，将HTTP中终端设备类型等字段的乱码清洗掉，并且按天去重
 * @author Administrator
 *
 */
public class DataCleaningTerminal extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 if (args.length != 2){

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
		    	
		    //读取爬虫配置数据 Phone
		    String confuriPhone = "/user/hive/warehouse/aotain_dim.db/to_ref_phoneinfo/";
		    FileSystem fsPhone = FileSystem.get(URI.create(confuriPhone),conf); 
            FSDataInputStream inPhone = null; 
            ArrayList<String> keylistPhone = new ArrayList<String>();
            Map<String,String> map=new HashMap<String,String>();
            try{
            	Path path = new Path(confuriPhone);
            	if(fsPhone.exists(path))
            	{
            		System.out.println(confuriPhone); 
            		System.out.println("exist conf file !!!!!!"); 
            		try{
            	  
            			if(fsPhone.isDirectory(path))
            			{            		
            				for(FileStatus file:fsPhone.listStatus(path))
            				{
            					//file.readFields(in);
            					inPhone = fsPhone.open(file.getPath());
    	            			BufferedReader bis = new BufferedReader(new InputStreamReader(inPhone,"UTF8")); 
    	            			String line = "";
    	            			while ((line = bis.readLine()) != null) {  
    	            				String[] arr = line.split(",",-1);
    	            				if(arr[1].contains("型号"))
    	            				{
    	            					//String str = "Phone" + ":" +arr[0]+":"+arr[2];
	    	            				//if(!keylistPhone.contains(str))
	    	            				{
	    	            					//keylistPhone.add(str);
	    	            					map.put("Phone" + ":" +arr[0], arr[2]);
	    	            				}
    	            				}
    	            			}
            				}
	            			
            			}
            			//deviceList.deleteCharAt(deviceList.length()-1);//去掉最后一个逗号
            			//System.out.println(deviceList.toString()); 
	             
            	  }finally{
            	  IOUtils.closeStream(inPhone);
            	  }
            	}
            	else
            	{
            		System.out.println(confuriPhone); 
            		System.out.println("not exist file !!!!!!"); 
            	}
            }finally{ 
                IOUtils.closeStream(inPhone); 
            }
            //for(String key:keylistPhone)
           // {
            //	deviceList.append(key + "|");
            //}
            
            
            //读取爬虫配置数据 Pad
		    String confuriPad = "/user/hive/warehouse/aotain_dim.db/to_ref_padinfo/";
		    FileSystem fsPad = FileSystem.get(URI.create(confuriPad),conf); 
            FSDataInputStream inPad = null; 
            ArrayList<String> keylistPad = new ArrayList<String>();
            
            try{
            	Path path = new Path(confuriPad);
            	if(fsPad.exists(path))
            	{
            		System.out.println(confuriPad); 
            		System.out.println("exist conf file !!!!!!"); 
            		try{
            	  
            			if(fsPad.isDirectory(path))
            			{            		
            				for(FileStatus file:fsPad.listStatus(path))
            				{
            					//file.readFields(in);
            					inPad = fsPad.open(file.getPath());
    	            			BufferedReader bis = new BufferedReader(new InputStreamReader(inPad,"UTF8")); 
    	            			String line = "";
    	            			while ((line = bis.readLine()) != null) {  
    	            				String[] arr = line.split(",",-1);
    	            				if(arr[1].contains("型号"))
    	            				{
    	            					//String str = "Pad" + ":" +arr[0]+":"+arr[2];
	    	            				//if(!keylistPad.contains(str))
	    	            				//{
	    	            					//keylistPad.add(str);
	    	            					map.put("Pad" + ":" +arr[0], arr[2]);
	    	            				//}
    	            				}
    	            			}
            				}
	            			
            			}
            			//deviceList.deleteCharAt(deviceList.length()-1);//去掉最后一个逗号
            			//System.out.println(deviceList.toString()); 
	             
            	  }finally{
            	  IOUtils.closeStream(inPad);
            	  }
            	}
            	else
            	{
            		System.out.println(confuriPad); 
            		System.out.println("not exist file !!!!!!"); 
            	}
            }finally{ 
                IOUtils.closeStream(inPad); 
            }
            //for(String key:keylistPad)
            //{
            //	deviceList.append(key + "|");
            //}
            
            //System.out.println(deviceList.toString()); 
            //conf.set("app.devicelist", deviceList.toString());  
            conf.set("app.devicelist",
	                ObjectSerializer.serialize((Serializable) map));
            
		    Job job = Job.getInstance(conf);
		    
		    job.setJobName("Data Cleaning Terminal");                  
		
		    job.setJarByClass(getClass());
		
		    FileInputFormat.addInputPath(job,new Path(args[0]));
		    
		    FileOutputFormat.setOutputPath(job,new Path(args[1]));
		    
		    job.setInputFormatClass(LzoTextInputFormat.class);
		
		    job.setMapperClass(DataCleaningTerminalMapper.class);
		    job.setReducerClass(DataCleaningTerminalReducer.class);   
		    job.setOutputKeyClass(Text.class);
		    //job.setNumReduceTasks(128);
		    job.setOutputValueClass(Text.class);  
		    
		    return job.waitForCompletion(true)?0:1;   
	}

	public static void main(String [] args) throws Exception{
        int mr;
        mr = ToolRunner.run(new Configuration(),new DataCleaningTerminal(),args);
        System.exit(mr);
    }
}

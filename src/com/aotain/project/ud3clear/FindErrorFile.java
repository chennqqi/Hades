package com.aotain.project.ud3clear;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;





public class FindErrorFile {
   public static void main(String[] args) {
	  try{
		
	   String confuri= args[0];
		 Configuration conf = new Configuration();
		  conf.addResource(new Path("/etc/hadoop/conf/core-site.xml")); 
	      conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml")); 
	      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml")); 

		//  Logger.getLogger(CUDataImportBolt.class).info("###HDFS :" + conf.get("fs.defaultFS"));
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		    
    	//IPCYW9971632,218.17.90.26,218.17.90.62,255.255.255.192,
       FileSystem fs = FileSystem.get(conf); 
       FSDataInputStream in = null; 
    Path path = new Path(confuri);
    if(fs.exists(path))
    {
    	System.out.println(confuri); 
    	System.out.println("exist conf file !!!!!!"); 
    	try{
    		if(fs.isDirectory(path))
    		{            		
    			for(FileStatus file:fs.listStatus(path))
    			{
    				try{
		           			new FindErrorFile().finderror(fs, file.getPath(), file.getLen());
		           		}catch(Exception e){
		           			//e.printStackTrace();
		           			String arg=path+"/"+file.getPath().getName();
		           			System.out.println(arg);
		           			String cmd="hadoop fs -rm ";
		           		 cmd = cmd + "  " + arg;
		           	    String[] cmds = { "/bin/bash", "-c", cmd };
		           	 Runtime run = Runtime.getRuntime();
		           	Process    p = run.exec(cmds);
		           			
		           			
		           		}
    			}
    		}
    	}finally{ 
            IOUtils.closeStream(in); 
    	}
    }
    	else
    	{
    		System.out.println(confuri); 
    		System.out.println("not exist file !!!!!!"); 
    	}
    }catch(Exception E){
    	
    }
}
   public void finderror(FileSystem fs, Path path, long maxFileLength) throws Exception{
	   FSDataInputStream file = fs.open(path);
       long size;
       if(maxFileLength == 9223372036854775807L)
           size = fs.getFileStatus(path).getLen();
       else
           size = maxFileLength;
       int readSize = (int)Math.min(size, 16384L);
       file.seek(size - (long)readSize);
       ByteBuffer buffer = ByteBuffer.allocate(readSize);
       file.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
       int psLen = buffer.get(readSize - 1) & 255;
   }
  
}

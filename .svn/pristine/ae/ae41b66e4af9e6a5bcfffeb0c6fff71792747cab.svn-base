package com.aotain.project.ud3clear;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;


import com.hadoop.compression.lzo.LzopCodec;

public class Taskimpl extends Task{
     int count=0;
     String  outpath;
     String  filename;
     String hdfsoutpath;
     String timepath;
     List<String> datas=new ArrayList<String>();
     public Taskimpl(String  outpath, String  filename, String hdfsoutpath,
     String timepath,List<String> datas){
    	 this.outpath=outpath;
    	 this.filename=filename;
    	 this.hdfsoutpath=hdfsoutpath;
    	 this.timepath=timepath;
    	 this.datas.addAll(datas);
     }
   
	 public void run() {
		 /*try {
			 long filetime=0;
			 int radom=RandomUtils.nextInt(1000);
			 long time=System.currentTimeMillis();
			 SimpleDateFormat dfTime = new SimpleDateFormat("yyyyMMddHHmmss");
			
			 
			 String stime=dfTime.format(new Date(time));
				 filetime=time+radom;
				 String ip=InetAddress.getLocalHost().getHostAddress(); 
				  String filename=ip+"_"+stime+"_"+filetime;
<<<<<<< .mine

	

				  Configuration conf = new Configuration();
				  conf.addResource(new Path("/etc/hadoop/conf/core-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml")); 
		
			//	  Logger.getLogger(CUDataImportBolt.class).info("###HDFS :" + conf.get("fs.defaultFS"));
					conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				  String path = "hdfs://nameservice1"+hdfsoutpath+"/"+timepath ;
				  FileSystem hadoopFS = FileSystem.get(conf);
		            Path hadPath=new Path(path);
		      File file=new File(filename);
		            
				    if(!hadoopFS.exists(hadPath))
				    {
				    	boolean lb = hadoopFS.mkdirs(hadPath);
				    	Logger.getLogger(Taskimpl.class).info("###Create path:" + path + "  " + String.valueOf(lb));
				    }
				    FSDataOutputStream fsOut = hadoopFS.create(new Path(path+"/"+file.getName()+".txt.lzo")); 
			   LzopCodec lzo = new LzopCodec();
		            lzo.setConf(conf);
		           
		          CompressionOutputStream lzoOut = lzo.createOutputStream(fsOut);
		        String line=null;
			     for(int i=0;i<datas.size();i++){
			    	 line=datas.get(i);
			    	  ByteArrayInputStream inputStream 
		            	= new ByteArrayInputStream(line.getBytes("UTF-8"));
			    	  String line2 = null;
			    	  BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				        while ((line = br.readLine()) != null) { 
				        	line2 = line2 + "\n";
				            byte[] bytes = line.getBytes();  
				            lzoOut.write(bytes, 0, bytes.length);  
				        }
			     }
			     lzoOut.close(); 
			     fsOut.close();
			     }catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					};

		 File f1=new File(outpath);
		    if(!f1.exists()){
		    	f1.mkdir();
		    }
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
					outpath+"/"+filename),true));
			for(int i=0;i<datas.size();i++){
				writer.write(datas.get(i)+"\r\n");
=======
				  Configuration conf = new Configuration();
				  conf.addResource(new Path("/etc/hadoop/conf/core-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml")); 
			      conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml")); 
	/*		      conf.set("mapred.job.tracker", "192.168.102.136:9001");  
			      conf.set("fs.default.name", "hdfs://192.168.102.136:9000");  
				  Logger.getLogger(CUDataImportBolt.class).info("###HDFS :" + conf.get("fs.defaultFS"));
					conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				  String path = "hdfs://nameservice1"+hdfsoutpath+"/"+timepath ;
				  FileSystem hadoopFS = FileSystem.get(conf);
		            Path hadPath=new Path(path);
		            File file=new File(filename);
		            
				    if(!hadoopFS.exists(hadPath))
				    {
				    	boolean lb = hadoopFS.mkdirs(hadPath);
				    	Logger.getLogger(Taskimpl.class).info("###Create path:" + path + "  " + String.valueOf(lb));
				    }
				    FSDataOutputStream fsOut = hadoopFS.create(new Path(path+"/"+file.getName()+".txt")); 
				    LzopCodec lzo = new LzopCodec();
		            lzo.setConf(conf);
		           
		           // CompressionOutputStream lzoOut = lzo.createOutputStream(fsOut);
		           String line=null;
			     for(int i=0;i<datas.size();i++){
				  line=line + "\n";
				   byte[] bytes = line.getBytes("UTF-8"); 
				   fsOut.write(bytes);
				// lzoOut.write(bytes, 0, bytes.length);  
				   //Snappy.compress(bytes);
				 
>>>>>>> .r43519

			}
			  // lzoOut.close();  

	            fsOut.close();
	           
	
			//writer.close();
			 System.err.println("put into hdfs1 "+outpath+"/"+filename+" "+hdfsoutpath+"/"+timepath+"-------datasize"+datas.size());
			datas.clear();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
   */
	  
	 
	  }
	@Override
	public Task taskCore() throws Exception {
		
	/*	Task tas=new Taskimpl();
		System.out.println("satart second:"+count+10);*/
		return null;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		return null;
	}

}

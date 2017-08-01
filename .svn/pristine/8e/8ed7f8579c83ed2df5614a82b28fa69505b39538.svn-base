package com.aotain.project.ud3clear;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.compress.SnappyCodec;


public class UserMDF  extends Configured implements Tool {

	private static final Log log = LogFactory.getLog(UserMDF.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
		 HashMap<String,String> AdslMap = new HashMap<String,String> ();
		 HashMap<String,String> classMap = new HashMap<String,String> ();
		  @Override
		  public void setup(Context context) throws IOException,
		  InterruptedException {
			  String city = context.getConfiguration().get("city");
				 String partdate = context.getConfiguration().get("partdate");
		         String confuri="/user/hive/warehouse/aotain_dim.db/to_ref_areanetwork/"+city+"/"+partdate;
		         Configuration config = context.getConfiguration();
				    
		      	//IPCYW9971632,218.17.90.26,218.17.90.62,255.255.255.192,
		         FileSystem fs = FileSystem.get(URI.create(confuri),config); 
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
				           		System.out.println(confuri); 
				           		System.out.println("exist conf file !!!!!!"); 
				           		try{
				           			//file.readFields(in);
				           			in = fs.open(file.getPath());
				    	           	BufferedReader bis = new BufferedReader(new InputStreamReader(in,"UTF8")); 
				    	           	String line = "";
				    	           	while ((line = bis.readLine()) != null) {  
				    	           		String items[] = line.split(",",-1);
				    	           		AdslMap.put(items[1], items[0]);//IP,ADSL
				    	           	}
				           			//deviceList.deleteCharAt(deviceList.length()-1);//去锟斤拷锟斤拷锟揭伙拷锟斤拷锟斤拷锟�
				           			//System.out.println(deviceList.toString()); 
					            
				           		}finally{
				           	  IOUtils.closeStream(in);
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
		      setClassMap( context);
		}
	public void setClassMap(Context context) throws IOException{
		 String confuri="/user/hive/warehouse/dmp.db/url_class/";
         Configuration config = context.getConfiguration();
		    
      	//IPCYW9971632,218.17.90.26,218.17.90.62,255.255.255.192,
         FileSystem fs = FileSystem.get(URI.create(confuri),config); 
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
		           		System.out.println(confuri); 
		           		System.out.println("exist conf file !!!!!!"); 
		           		try{
		           			//file.readFields(in);
		           			in = fs.open(file.getPath());
		    	           	BufferedReader bis = new BufferedReader(new InputStreamReader(in,"UTF8")); 
		    	           	String line = "";
		    	           	while ((line = bis.readLine()) != null) {  
		    	           		String cols[] = line.split(",",-1);
		    	           	 if(cols.length>3){
		    	           		String domain=new URL(cols[3]).getHost();
		    	           		classMap.put(domain, cols[1]);
		    	           	 }
		    	           		//IP,ADSL
		    	           	}
		           			//deviceList.deleteCharAt(deviceList.length()-1);//去锟斤拷锟斤拷锟揭伙拷锟斤拷锟斤拷锟�
		           			//System.out.println(deviceList.toString()); 
			            
		           		}finally{
		           	  IOUtils.closeStream(in);
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
	}
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
			try{
	      
				String line = value.toString();			
				String items[]  = line.split("\\|", -1);
				  StringBuilder sb = new StringBuilder();
				  int count =items.length;
				  if(count<19)
					  return;
				  String category=items[14];
				  if(category!=null&&category.length()>10)
					  return;
				 for(int i = 0;i< 21;i++)
				 {
					 
					 String col = items[i];
					if(i==1 && !items[2].trim().isEmpty() 
							 && AdslMap.containsKey(items[2].trim()))
					 {
						 String username = AdslMap.get(items[2].trim());
						 col = username;
						
					 }
					if(i==14){
					 if(classMap.get(items[3])!=null)
						 col=classMap.get(items[3]);
				         else
				        	 col="0";
					}
					if(i==16){
				         if(classMap.get(items[15])!=null)
				        	 col=classMap.get(items[15]);
				         else
				        	 col="0";
				         }
					 if("".equals(col))
						 col=" ";
			         sb.append(col + "|");
			       }
					context.write(new Text (sb.toString()), new Text (""));
			 }catch(Exception e){
				 
			 }
		
		}
	}
	
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			
			context.write(key,new Text(""));
			
		}
	}

	
	/** 鍒濆鍖朖OB
	 * @param
	 * args[0]=domain  args[1]=outputpath args[2]=inputpath
	 * jarname usermd5 /user/data/usermdf/out/20151201  /user/hive/warehouse/broadband.db/to_opr_http/shenzhen/20151201
	 */
	public Job UserMDFJob(final Configuration conf,final String[] args) throws IOException {		
			
		String[] statstamp = args[2].split("/");
		System.out.println();
		String jobname = ">>>UserMDFJob>>> "+args[0] + ">>>" + statstamp[statstamp.length-1];		
		String input = args[2];
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		job.setJarByClass(UserMDF.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		//job.setInputFormatClass(TextInputFormat.class);

		job.setInputFormatClass(TextInputFormat.class);	
		job.setOutputFormatClass(OrcNewOutputFormat.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		job.setReducerClass(UserMDFReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job;
	}
	
	
	/**
	 * @param errorMsg Error message. Can be null.
	 */
	private static void usage(final String errorMsg) {
		if (errorMsg != null && errorMsg.length() > 0) {
			log.error("ERROR: " + errorMsg);
		}
		log.info("Usage: hadoop jar XX.jar jobname output inputlist");

	}
	
	public static void main(final String[] args) throws Exception {
		log.info("-------UserMDF main start----");
		PropertyConfigurator.configure("../conf/log4j.properties");
		int exitCode = ToolRunner.run(new UserMDF(), args);
		
		System.exit(exitCode);
	}
	
	/** 
	 * @param
	 * args[0]=jobname  args[1]=outputpath args[2]=inputpath args[3]=impalanode args[4]=province 
	 */
	public int run(String[] args) throws Exception {
		conf = new Configuration();	
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		for(int i=0;i<otherArgs.length;i++){
			log.info(otherArgs[i]);
		}
		if (otherArgs.length != 5) {
			usage("Wrong number of arguments: " + otherArgs.length);
			System.exit(-1);
		}		
		String city=otherArgs[3];
		String partdate=otherArgs[4];
		conf.set("city", city);
		conf.set("partdate", partdate);
		Job statics = UserMDFJob(conf,otherArgs);
		int ret = statics.waitForCompletion(true) ? 0 : 1;		
		return ret;
		

		
	}
}




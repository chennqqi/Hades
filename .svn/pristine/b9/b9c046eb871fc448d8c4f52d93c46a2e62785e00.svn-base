package com.aotain.project.dmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.hadoop.mapreduce.LzoIndexOutputFormat;


public class KeyWordCrawlerData  extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(KeyWordCrawlerData.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
		
		   private void geChooseConfig(){
			/* kewordMap.put("guangmingxinqu", "光明新区,行政区");
		    	  kewordMap.put("longhuaxinqu", "龙华新区,行政区");
		    	  kewordMap.put("dapengxinqu", "大鹏新区,行政区");
		    	  kewordMap.put("futian", "福田,行政区");
		    	  kewordMap.put("luohu", "罗湖,行政区");
		    	  kewordMap.put("nanshan", "南山,行政区");
		    	  kewordMap.put("yantian", "盐田,行政区");
		    	  kewordMap.put("baoan", "宝安,行政区");
		    	  kewordMap.put("longgang", "龙岗,行政区");
		    	  kewordMap.put("huizhoua", "惠州,行政区");
		    	  kewordMap.put("dongguanc", "东莞,行政区");
		    	  //住宅 经济适用房 别墅 写字楼 两限房商铺公租房安居房综合体
		    	  kewordMap.put("t13", "住宅,类型");
		    	  kewordMap.put("t14", "住宅,类型");
		    	  kewordMap.put("t15", "别墅,类型");
		    	  kewordMap.put("写字楼", "类型");
		    	  kewordMap.put("商铺", "类型");*/
			      String fileName="/user/project/dmp/config";
			try {

				FileSystem fs = FileSystem.get(URI.create(fileName), conf);
				FSDataInputStream in = null;
				BufferedReader fis = null;

				try {
					Path path = new Path(fileName);
					if (fs.exists(path)) {
						System.out.println(fileName);
						System.out.println("exist conf file !!!!!!");
						if(fs.isDirectory(path))
				      		{   
							for(FileStatus file:fs.listStatus(path))
				      			{
						           		System.out.println(fileName); 
						           		System.out.println("exist conf file !!!!!!"); 
						           		Path filepath=file.getPath();
						           		String filename=filepath.getName();
								in = fs.open(filepath);
								fis = new BufferedReader(new InputStreamReader(in,
										"UTF-8"));
								// fis = new BufferedReader(new FileReader(fileName));
								String pattern = null;
								// int i = 0;
								while ((pattern = fis.readLine()) != null) {
										String cols[] = pattern.split("\\|");
										String value = cols[1];
										if (value.indexOf(":") != -1)
											value = value.substring(0, value.indexOf(":"));
										kewordMap.put(filename+cols[0], value);
										System.out.println(cols[0]);
									}
				      			}
					        }
						}
				      } catch (IOException ioe) {
				        System.err.println("Caught exception while parsing the  file '");
				        ioe.printStackTrace();
				      }finally{
				    	  if(in != null){
				    		  in.close();
				    	  }
				    	  if(fis != null){
				    		  fis.close();
				    	  }
				      }
			      }catch(Exception e){
			    	  e.printStackTrace();
			      }
		    	
		   }
		   public Map<String,String> kewordMap =new HashMap<String,String>();
		      @Override
			  public void setup(Context context) throws IOException,
			  InterruptedException {
		    	  conf = context.getConfiguration();
		    	  geChooseConfig();
		    	 
		    	  
		    	  
		    	  
		   }	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
			try{
	      
				String line = value.toString();			
				String items[]  = line.split(",", -1);
				 if(items.length==20){
			    String url=items[1];
			    String username=items[0];
			    if(url.indexOf("http://shenzhen.anjuke.com/sale/")!=-1){
			    	outResult("shenzhen-anjuke-esf.dic",1,1,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://shenzhen.anjuke.com/loupan/")!=-1){
			    	outResult("shenzhen-anjuke-xf.dic",1,2,"&",url,context,username,kewordMap);
			    }else if(url.indexOf("http://esf.sz.fang.com/")!=-1){
			    	outResult("shenzhen-fang-esf.dic",0,1,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://newhouse.sz.fang.com/house/s/")!=-1){
			    	outResult("shenzhen-fang-xf.dic",2,1,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://shenzhen.qfang.com/sale/")!=-1){
			    	outResult("shenzhen_qfang_esf.txt",1,1,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://shenzhen.qfang.com/newhouse/list")!=-1){
			    	outResult("shenzhen_qfang_xf.txt",2,1,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://sz.lianjia.com/ershoufang/")!=-1){
			    	outResult("shenzhen-lianjia-esf.dic",1,3,"-",url,context,username,kewordMap);
			    }else if(url.indexOf("http://sz.fang.lianjia.com/loupan/")!=-1){
			    	outResult("shenzhen-lianjia-xf.dic",1,3,"-",url,context,username,kewordMap);
			    }
				 }
			    
					
			 }catch(Exception e){
				 
			 }
		
		}
	}
	
	
	private static void outResult(String pathname,int startpostion,int flag ,String splitflag,String url,org.apache.hadoop.mapreduce.Mapper.Context context,String username,Map<String,String> kewordMap) throws Exception{
		String paths[]=new URL(url).getPath().split("/");
    	int pathlength=paths.length;
    
    	if(pathlength>1){
	    	for(int i=startpostion;i<pathlength;i++){ 
	    		String path2[]=null;
	    		if(flag==1){
	    			if(!paths[i].contains("house"))
	    		    path2=paths[i].split(splitflag);
	    		    }
	    	
	    		else if(flag==3){
	    			path2=new String[4];
	    			 Pattern p = Pattern.compile("nhtt[0-9]{1,2}|nht[0-9]{1}|p[0-9]{1}|l[0-9]{1}");
	    		        Matcher m = p.matcher(paths[i]);  
	    		        int c=0;
	    		        while (m.find()) {
	    		        	path2[c]=m.group();
	    		        	c++;
	    		        } 
	    		     
	    		    	
		    			
	    		}
		    		if(path2!=null&&path2[0]!=null&&path2.length>1){
		    			for(int j=0;j<path2.length;j++){
		    				String category=kewordMap.get(pathname+path2[j]);
		    				 if(category!=null)
		    					 context.write(new Text (username+","+category), new Text ("1"));
		    				 }
		    				  
		    		}else{
			    		String category=kewordMap.get(pathname+paths[i]);
			    		 if(category!=null)
			    			 context.write(new Text (username+","+category), new Text ("1"));
	    				  
			    	
	    		}
	    	}
    	
    	}
    	   if(flag==2){
    			String path2[]=null;
			path2=new URL(url).getQuery().split(splitflag);
			for(int j=0;j<path2.length;j++){
				String category=kewordMap.get(pathname+path2[j]);
				 if(category!=null)
					 context.write(new Text (username+","+category), new Text ("1"));
				  
			}
			
		}
	}

	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {
	
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			int count=0;
			 for(Text value:values){
				 count++;
					}
			 count=count*3;
			 String skey=key.toString();
			 context.write(new Text(skey+","+count+","),new Text(""));
			
		}
	}

	
	/** 初始化JOB
	 * @param
	 * args[0]=domain  args[1]=outputpath args[2]=inputpath
	 * jarname usermd5 /user/data/usermdf/out/20151201  /user/hive/warehouse/broadband.db/to_opr_http/shenzhen/20151201
	 */
	public Job UserMDFJob(final Configuration conf,final String[] args) throws IOException {		
		
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");		
		String[] statstamp = args[2].split("/");
		System.out.println();
		String jobname = ">>>crawler_data>>> "+args[0] + ">>>" + statstamp[statstamp.length-1];		
		String input = args[2];
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		job.setJarByClass(KeyWordCrawlerData.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setInputFormatClass(LzoTextInputFormat.class);		
		//FileOutputFormat.setCompressOutput(job, true);
		//FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		
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
		int exitCode = ToolRunner.run(new KeyWordCrawlerData(), args);
		
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




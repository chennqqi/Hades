package com.aotain.project.dmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.hadoop.mapreduce.LzoIndexOutputFormat;


public class CarCrawlerData  extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(CarCrawlerData.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
		
		
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
		       try{
	      
				String line = value.toString();			
				String items[]  = line.split(",", -1);
				
			    if(items.length<3)
			    	return;
			    String carid=items[0];
			    String []attname={"汽车价格","厂商","品牌","车型","车系","级别","油耗","排量","驱动方式","车门数","座位数","国家","变速箱类型","最大马力"};
			   /* if(items.length!=attname.length)
			    	return;*/
			    Map<String,String>map=new HashMap<String, String>();
			    map.put("厂商", "1|华晨宝马");
			    map.put("车型", "1|华晨宝马");
			    map.put("车系", "1|华晨宝马");
			    map.put("驱动方式", "2|四驱,后驱,前驱");
			    map.put("汽车价格", "3|0_5_5万以下,5_10_5-10万,10_15_10-15万,15_20_15-20万,20_5_20-25万,25_35_25-35万,35_50_35-50万,50_100_50-100万,100_100000_100万以上");
			    map.put("品牌", "1|宝马5系");
			    map.put("车门数", "1|2");
			    map.put("座位数", "4|座,座以上");
			    map.put("排量", "3|0_1.1_1.1L以下,1.1_1.7_1.1-1.7L,1.7_2.0_1.7-2.0L,2.0_2.5_2.0-2.5L,2.5_500000_2.5L以上");
			    map.put("级别", "5|微型车,小型车,紧凑型车,中型车,中大型车,大型车,SUV,MPV,跑车,微面,微卡,轻客,皮卡");
			    map.put("变速箱类型", "2|手动,自动");
			    map.put("国家", "2|德国,日本,韩国,美国,中国");
			    map.put("油耗", "3|6_7_6-7L,7_8_7-8L,8_9_8-9L,9_10_9-10L,10_500000_10L以上,0_6_6L以下");
			    map.put("最大马力", "3|160_180_160-180,0_160_160以下,180_200_180-200,200_10000000_200以上");
	             String outkey="";
			    for(int i=0;i<attname.length;i++){
	            	String []values=map.get(attname[i]).split("\\|");
	            	String flag=values[0];
	            	if("1".equals(flag)){
	            		if(!"".equals(items[i+1])){
	            		outkey=carid+","+attname[i]+","+items[i+1]+",";
	            		context.write(new Text (outkey), new Text (""));
	            		}
	            	}
	            	else if("2".equals(flag)){
	            		String[] outVlause=values[1].split(",");
	            		for(int j=0;j<outVlause.length;j++){
	            			if(items[i+1].contains(outVlause[j])){
	            				outkey=carid+","+attname[i]+","+outVlause[j]+",";
	            				context.write(new Text (outkey), new Text (""));
	            			}
	            		}
	            		
	            	}
	            	else if("3".equals(flag)){
	            		String[] outVlause=values[1].split(",");
	            		
	            		String[] prices=items[i+1].split("-");
	            		if(prices.length>0){
	            		   String price=prices[0];
		        			boolean fflag=isMatch("^[0-9\\.]+$",price);
		            		if(fflag){
		            		 float number=Float.parseFloat(price);
			            		for(int j=0;j<outVlause.length;j++){
			            			String []numvalue=outVlause[j].split("_");
			            			float pre=Float.parseFloat(numvalue[0]);
			            			float after=Float.parseFloat(numvalue[1]);
			            			if(number>=pre&&number<after){
			            				outkey=carid+","+attname[i]+","+numvalue[2]+",";
			            				context.write(new Text (outkey), new Text (""));
			            			 }
			            		}
			            		}
	            		}
	            	}else if("4".equals(flag)){
	            		boolean fflag=isMatch("^[0-9\\.]+$",items[i+1]);
	            		if(fflag){
	            		int count=Integer.parseInt(items[i+1]);
	            		if(count<=7)
	            		   outkey=carid+","+attname[i]+","+items[i+1]+"座,";
	            		else
	            			outkey=carid+","+attname[i]+",7`座以上,";
	            		context.write(new Text (outkey), new Text (""));
	            		}
	            	}
	            	else if("5".equals(flag)){
	            		String[] outVlause=values[1].split(",");
	            		for(int j=0;j<outVlause.length;j++){
	            			if(items[i+1].contains(outVlause[j])){
	            				if(items[i+1].contains("SUV")){
	            				outkey=carid+","+attname[i]+","+outVlause[j]+",";
	            				context.write(new Text (outkey), new Text (""));
	            				break;
	            				}else{
	            					outkey=carid+","+attname[i]+","+outVlause[j]+",";
		            				context.write(new Text (outkey), new Text (""));
	            				}
	            			}
	            		}
	            	}
	            }
			   }catch(Exception e){
				   
			   }
			   // outkey=loupanid+",开发商,"+produce+","+housename+",";
			    //context.write(new Text (outkey), new Text (""));
			  
				
				//context.write(new Text (outkey), new Text (""));
				
					
			
		
		}
	}
	
	private static   boolean isMatch(String regex,String prestring){
		boolean flag=false;
		
		Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		
        Matcher m = p.matcher(prestring); 
       if(m.find()){
        	 flag=true;
        }
		return flag;
		}
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			context.write(key,new Text(""));
			
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
		job.setJarByClass(CarCrawlerData.class);
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
		//PropertyConfigurator.configure("../conf/log4j.properties");
		int exitCode = ToolRunner.run(new CarCrawlerData(), args);
		
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




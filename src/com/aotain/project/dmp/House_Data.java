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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;

import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import com.hadoop.mapreduce.LzoIndexOutputFormat;


public class House_Data  extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(House_Data.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
		
   public Map<String,String> domainMap =new HashMap<String,String>();
      @Override
	  public void setup(Context context) throws IOException,
	  InterruptedException {
    	
  		  domainMap.put("anjuke.com", "anjuke,http://[a-z]+\\.fang\\.anjuke\\.com/loupan/[A-Za-z\\-]*([0-9]+)\\.html\\??,1");
      	  //|http://[a-z]+\\.fang\\.anjuke\\.com/loupan/[A-Za-z]+-([0-9]+)\\.html\\?from=loupan_tab
      	  domainMap.put("fang.com/bbs", "soufang,http://([A-Za-z0-9]+)\\.fang\\.com/bbs/,1");
      	  domainMap.put("fang.com/house", 
                     "soufang,http://([A-Za-z0-9]+)\\.fang\\.com/house/yh[0-9_]+\\.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/housedetail.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/fangjia.htm$|http://([A-Za-z0-9]+)\\.fang\\.com/house/[0-9]+/dongtai.htm$,1");
      	  domainMap.put("fang.com/photo", "soufang,http://([A-Za-z0-9]+)\\.fang\\.com/photo/[A-Za-z0-9_]+\\.htm$^?,1");
      	  domainMap.put("fang.com/dianping", "soufang,http://([A-Za-z0-9]+)\\.fang\\.com/dianping/$,1");
      	  domainMap.put("fang.com/zhuangxiu", "soufang,http://([A-Za-z0-9]+)\\.fang\\.com/zhuangxiu/$,1");
      	  domainMap.put("fang.com", "soufang,^http://([A-Za-z0-9]+)\\.fang\\.com$,2");
      	  domainMap.put("lianjia.com", "lianjia,http://[a-z]+\\.fang\\.lianjia\\.com/loupan/p_([A-Za-z0-9]+)/?\\??,1");
      	  domainMap.put("qfang.com", "qfang,http://[a-z]+\\.qfang\\.com/newhouse/[A-Za-z]+/([0-9]+)\\??,1");
    	  }
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
			try{
	      
				 String line = value.toString();			
				 String items[]  = line.split("\\|", -1);
				 if(items.length==20){
				 StringBuilder sb = new StringBuilder();
				 String url=items[4];
				 String domain=items[3];
				 String accesstime=items[11];
				 String username=items[1];
				 
				 String sys="";
				 if(items[6]!=null)
					 sys= items[6].toLowerCase().trim();
				 int flag=2;
				 if(sys.equals("windows")||sys.equals("macintosh")){
					 flag=1;
				 }
				
				
					String domains[]= domain.split("\\.");
					String rootDomain=domains[domains.length-2]+"."+domains[domains.length-1];
					String rules=domainMap.get(rootDomain);
					String referDomain=items[15];
					if(referDomain.contains(rootDomain)||" ".equals(referDomain)){
						referDomain="1";
					}
					if(rules!=null){
						String[] result=	paraseRules( rules, url);
						String match="";
						int jflag=Integer.parseInt(result[2]);
						if(jflag==1){
							match=result[3];
						}
			          else if(jflag==2){
							try {
								String paths[]=new URL(url).getPath().split("/");
								if(paths.length>1){
									rootDomain=rootDomain+"/"+paths[1];
									 rules=domainMap.get(rootDomain);
									 if(rules!=null){
										 String result2[]=paraseRules( rules, url);
										 match=result2[3];
										 if("".equals(match)){
											 match=result[3]; 
										 }
									 }else{
										 match=result[3];  
									 }
								 }else{
									 match=result[3];  
								 }
							} catch (MalformedURLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					
						 String outkey=username+","+url+","+match+","+result[0]+",T1,"+domain+","+accesstime+","+flag+","+referDomain+",";
						 if(!"".equals(match))
						 context.write(new Text (username), new Text (outkey));  
					}
				 
				 }
			 }catch(Exception e){
				 
			 }
			
		}
	}
	private static String[] paraseRules(String rules,String url){
		String[]result=new String[4];
		String source=rules.split(",")[0];
		String regex=rules.split(",")[1];
	    String jflag=rules.split(",")[2];
	    result[0]=source;
	    result[1]=regex;
	    result[2]=jflag+"";
	    
		String match="";
	       
			if(regex.contains("|")){
				String regexs[]=regex.split("\\|");
				for(int i=0;i<regexs.length;i++){
					match=findMathc(url,regexs[i]);
					if(!"".equals(match)){
						match=match+"_"+source;
						break;
					}
				}
			}else{
				match=findMathc(url,regex);
				if(!"".equals(match)){
					match=match+"_"+source;
					
				}
				
				
			}
		
		result[3]=match;
		return result;
	}
	private static boolean isMatch(String url,String regex){
		Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(url); 
        return m.find();
	}
	private static String findMathc(String url,String regex){
		 Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
	        Matcher m = p.matcher(url); 
	        String match="";
	      
	        while (m.find()) {
	        	match=m.group(1);
	        } 
	        return match;
	}
	public static class UserMDFReducer extends Reducer<Text, Text, Text, Text> {		
		protected void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			
			 for(Text value:values){
			context.write(value,new Text(""));
			}
			
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
		String jobname = ">>>House_data>>> "+args[0] + ">>>" + statstamp[statstamp.length-1];		
		String input = args[2];
		Job job = Job.getInstance(conf);
		job.setJobName(jobname);
		job.setJarByClass(House_Data.class);
		for (String pt : input.split(",")) {
			FileInputFormat.addInputPath(job,new Path(pt));
		}
		
		job.setMapperClass(UserMDFMapper.class);
		Path outputpath = new Path(args[1]);
		outputpath.getFileSystem(conf).delete(outputpath, true);
		FileOutputFormat.setOutputPath(job, outputpath);
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(LzoTextInputFormat.class);		
		/*FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);*/
		
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
		int exitCode = ToolRunner.run(new House_Data(), args);
		
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




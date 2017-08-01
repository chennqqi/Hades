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


public class CrawlerData  extends Configured implements Tool {

	private final static String cut = "|";
	private static final Log log = LogFactory.getLog(CrawlerData.class);
	private static Configuration conf = null;
	
	
	public static class UserMDFMapper extends Mapper<LongWritable, Text, Text, Text> {	
		
		
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 
			try{
	      
				String line = value.toString();			
				String items[]  = line.split("\\|", -1);
			    if(items.length<3)
			    	return;
				String loupanid=items[3];
				String area=items[5];
				String housename=items[7];
				String regex="[^0-9a-zA-Z\u4E00-\u9FA5]+";
				housename=housename.replaceAll(regex, "").toLowerCase().replace(" ", "");
				housename=housename.replace("一期", "");	
				housename=housename.replace("二期", "");	
				housename=housename.replace("三期", "");	
				housename=housename.replace("四期", "");	
				housename=housename.replace("1期", "");	
				housename=housename.replace("2期", "");	
				housename=housename.replace("3期", "");	
				housename=housename.replace("4期", "");	
				String price=items[10];
				String characterr=items[8];
				String category=items[11];
				String situation=items[12];
			    String produce=items[13];
			    String outkey=loupanid+",行政区,"+area+","+housename+",";
			    if(!"".equals(area)&&!area.equals("行政区"))
			    context.write(new Text (outkey), new Text (""));
			    Pattern p = Pattern.compile("^[0-9]+$",Pattern.CASE_INSENSITIVE);
			    Matcher m = p.matcher(price.trim()); 
			    if(m.find()){
			    	int tprice=Integer.parseInt(price);
			    	if(tprice<20000){
			    		price="两万以下";
			    	}else if(tprice>=20000&&tprice<40000){
			    		price="两万-四万";
			    	}else if(tprice>=40000&&tprice<60000){
			    		price="四万-六万";
			    	}else if(tprice>=60000&&tprice<80000){
			    		price="六万-八万";
			    	}else if(tprice>=80000){
			    		price="八万以上";
			    	}
			    	  outkey=loupanid+",价格区间,"+price+","+housename+",";
			    	  
					    context.write(new Text (outkey), new Text (""));
			    }
			  
			    if(category.contains("别墅")){
			    	 outkey=loupanid+",类型,别墅,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(category.contains("商铺")){
			    	 outkey=loupanid+",类型,商铺,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(category.contains("商住")){
			    	 outkey=loupanid+",类型,商住,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(category.contains("写字楼")){
			    	 outkey=loupanid+",类型,写字楼,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(category.contains("住宅")){
			    	 outkey=loupanid+",类型,住宅,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(characterr.contains("地铁")||characterr.contains("线")){
			    	 outkey=loupanid+",特色,地铁房,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(characterr.contains("品牌")){
			    	 outkey=loupanid+",特色,品牌地产,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(characterr.contains("小户型")){
			    	 outkey=loupanid+",特色,小户型,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(characterr.contains("学校")||characterr.contains("学区房")){
			    	 outkey=loupanid+",特色,学区房,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(situation.contains("简装")){
			    	 outkey=loupanid+",装修情况,简装修,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if((situation.contains("精装")||situation.contains("全装修")||situation.contains("拎包入住"))&&!situation.contains("公共部分")){
			    	 outkey=loupanid+",装修情况,精装修,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(situation.contains("毛坯")||situation.contains("公共部分")){
			    	 outkey=loupanid+",装修情况,毛坯,"+housename+",";
			    	 context.write(new Text (outkey), new Text (""));
			    }
			    if(produce.contains("万科"))
			    	produce="万科";
			    else
			    	produce="其他";
			    outkey=loupanid+",开发商,"+produce+","+housename+",";
			    context.write(new Text (outkey), new Text (""));
			  
				
				//context.write(new Text (outkey), new Text (""));
				
					
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
		job.setJarByClass(CrawlerData.class);
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
		int exitCode = ToolRunner.run(new CrawlerData(), args);
		
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




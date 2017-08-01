package com.aotain.project.gdtelecom;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SpiderDeviceSegment extends Configured implements Tool {
    public int run(String[] args) throws Exception {                  
		if( args.length != 2) {
			return 1;
		}
		
    	Configuration conf = new Configuration(); 
		
		String inputPath = args[0];
		String targetPath = args[1];
		
		FileSystem fsTarget = FileSystem.get(URI.create(targetPath),conf);
        Path pathTarget = new Path(targetPath);
        if(fsTarget.exists(pathTarget))
        {
      	  	fsTarget.delete(pathTarget, true);
        }
		
        Job job = Job.getInstance(conf);
        job.setJobName("hdfs mr [SpiderDeviceSegment]");                    
        job.setJarByClass(getClass());
        job.setMapperClass(DVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);                  
        job.setInputFormatClass(TextInputFormat.class);
        job.setReducerClass(DVReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class); 
	    job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(targetPath));         

        return job.waitForCompletion(true)?0:1;                  
    }

    public static void main(String[] args)throws Exception{
         int exitcode = ToolRunner.run(new SpiderDeviceSegment(), args);
         System.exit(exitcode);                  
    }   
}

class DVMapper extends Mapper<LongWritable,Text,Text,Text> {
	private String regEx = "[`~!@$%^&*()+=|{}':;'//[//].<>/?~！@￥%……&*（）——+|{}【】‘；：”“’。、？ [\\u4e00-\\u9fa5]]"; 
	private String timeRegex =  ".*上市日期=((200)|(19)).*";// 上市时间2010前的不要

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] items;

		try{

			  items = value.toString().split("\\|");
			  if(items.length < 4){
				  return;
			  }
			  //spider alias model
			  String model = items[0].trim().toUpperCase();
			  //spider display name
			  String name = items[1].trim().toUpperCase();
			  String type = items[3].trim().toUpperCase();
			  //type brand
			  String prefix = type+"|"+items[2].trim().toUpperCase()+"|";
			  
			  // 没有价格的终端不要，如未上市、概念机
			  String price = items[4].trim();
			  if(!isNum(price)){
				  System.out.println("过滤没有价格的终端:" + value.toString());
				  return;
			  }
			  
			  // 2000年前的手机不要
			  String att = items[5];
			  if(type.equals("MOBILE") && isValid(timeRegex,att)){
				  System.out.println("过滤2010年前的手机:" + value.toString());
				  return;
			  }
			  
			  Pattern p = Pattern.compile(regEx);
			
			  if(null == model || model.equals("")){
				  model = parseTelTypeFromName(name);
				  if(model != null  && model.length() >0 ){
					  deviceSegment(context, model, name, prefix, p);
				  }
			  }else {
				  deviceSegment(context, model, name, prefix, p);
			  }
		
			  
		}catch(Exception E){
			;
		}
	}
	private void deviceSegment(
			Mapper<LongWritable, Text, Text, Text>.Context context,
			String model, String name, String prefix, Pattern p)
			throws IOException, InterruptedException {
		  Matcher m = p.matcher(model); 
		  String temp = m.replaceAll("").replace("，", ",");
		  String[] result = temp.split(",");
		  for(String tp : result){
			  if(tp.trim() !=null && tp.trim().length() >= 2){
				  context.write(new Text(prefix+tp), new Text(name));
			  }
		  }
	 }	
		public String parseTelTypeFromName(String name){
			//Moto RAZR V锋芒（MT887/移动版）
			String regex1 = "（([-a-zA-Z\\d\\s\\+]*)\\/";// 取（到/部分
			String regex2 = "([-a-zA-Z\\d\\s\\+]+)[（]";
			String regex3 = "([-a-zA-Z\\d\\s\\+]+)";
			String result = find(name, regex1);
			if(!isModel(result)) {
				result = find(name, regex2);
			} 
			if(!isModel(result)) {
				result = find(name, regex3);
			}
			if(!isModel(result)) {
				result = "";
			}
			return result.toUpperCase();
			
		}
	
	private boolean isNum(String s){
		if(s== null || s.trim().equals("")){
			return false;
		}
		s = s.replaceAll("万", "");
		try {
			Double d = Double.parseDouble(s);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	private static boolean isValid(String timeRegex , String att){
		Pattern p = Pattern.compile(timeRegex);
		return p.matcher(att).find();
	}
		
	private boolean isModel(String model) {
		return model != null && !model.contains("GB") && !model.contains("4G") && !model.contains("3G") && !model.contains("2G");
	}
	
	private String find(String str, String regex) {
		Pattern pat = Pattern.compile(regex);
		Matcher mat = pat.matcher(str);
		if(mat.find()){
			return mat.group(1);
		}
		return null;
	}
	
}

class DVReducer  extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		String alias = null;
		String rexname = null;
		Set<String> set = new HashSet<String>();
		String regEx ="\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）";
		for(Text temp : values){
			alias = temp.toString();
			Pattern  p = Pattern.compile(regEx);      
			Matcher m = p.matcher(alias); 
			rexname = m.replaceAll("").trim();
			set.add(rexname);
		}
		if(set.size() == 1 ){
			context.write(new Text(key+"|"+ alias+"|"), new Text(""));
		}
	}
	

}



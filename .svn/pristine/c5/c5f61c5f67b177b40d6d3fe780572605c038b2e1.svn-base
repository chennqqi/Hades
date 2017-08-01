package com.aotain.project.terminal;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.common.HfileConfig;


public class UserStaticInfoMR extends Configured implements Tool {
	
	
	  public static class UserStaticInfoMapper extends 
	  Mapper<Object, Text, Text, Text>{
		  private Configuration conf;
		  private BufferedReader fis;
		  private Map<String, String> moblieInfo = new HashMap<String,String>();
		  private Map<String, String> classInfo = new HashMap<String,String>();
		  private String[] cols;
		  private String cityName = "";
		  private String fieldsplit = "";
		  private String column = "";
		  private String date = "";
		  private int updatecyc ;
		  @Override
		  public void setup(Context context) throws IOException,
		  InterruptedException {
			  conf = context.getConfiguration();
			  fieldsplit = context.getConfiguration().get("fieldsplit");//列属性分隔符
			  column = context.getConfiguration().get("column");
			  date = context.getConfiguration().get("date");
			  cityName = context.getConfiguration().get("cityName");
			  updatecyc = Integer.parseInt(context.getConfiguration().get("updatecyc"));
			  
			  if (cityName !=null && !"".equals(cityName)){
				  parseFile(context.getConfiguration().get("mobilePath"));
			  }
			  parseClassName(context.getConfiguration().get("classNamePath"));
		}
		  private void parseClassName(String fileName) throws IOException {
			  FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
			  FSDataInputStream in = null; 
		      try {
		    	Path mobilePath = new Path(fileName);
		    	in = fs.open(mobilePath);
		        fis = new BufferedReader(new InputStreamReader(in,"UTF-8"));
		        String pattern = null;
		        int i = 0;
		        while ((pattern = fis.readLine()) != null) {
		        	 cols = pattern.split(",");
		        	 if(cols.length>3)
		     		 classInfo.put(cols[2], cols[3]);
		        }
		      } catch (IOException ioe) {
		        System.err.println("Caught exception while parsing the  file '");
		      }finally{
		    	  if(in != null){
		    		  in.close();
		    	  }
		    	  if(fis != null){
		    		  fis.close();
		    	  }
		      }
		  }
		  private void parseFile(String fileName) throws IOException {
			  FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
			  FSDataInputStream in = null; 
		      try {
		    	Path mobilePath = new Path(fileName);
		    	in = fs.open(mobilePath);
		        fis = new BufferedReader(new InputStreamReader(in,"GBK"));
		        String pattern = null;
		        while ((pattern = fis.readLine()) != null) {
		        	 cols = pattern.split("\\|");
		     		 if(cityName.equals(cols[3]))
		     			moblieInfo.put(cols[1], cols[1]);
		        }
		      } catch (IOException ioe) {
		        System.err.println("Caught exception while parsing the  file '");
		      }finally{
		    	  if(in != null){
		    		  in.close();
		    	  }
		    	  if(fis != null){
		    		  fis.close();
		    	  }
		      }
		    }
		  
		  @Override
		  public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
			  try{
				  String[] items = value.toString().split(fieldsplit,-1);
				  SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
				  Date date1 = format.parse(items[4]);
				  Date date2 = format.parse(date);
				  long day1 = date1.getTime();
				  long day2 = date2.getTime();
				  long days = (day2-day1)/1000/3600/24;
				  String attcode = items[1];
				  String attvalue = items[2].trim();
				  //根据配置用户属性流失周期淘汰一部分属性
				  if(items.length>4 && column.contains(attcode)&& days<updatecyc){
					  //手机归属地判断
					  if(attcode.equals("Phone") || attcode.equals("Telephone")){
						  if (moblieInfo.size()>0){
							  int al = attvalue.length();
							  switch (al) {
							  		//11位手机号
							  		case 11:
							  			if (moblieInfo.get(attvalue.substring(0,7)) == null)
							  				return;
							  			else
							  				break;
							  		//86+11位手机号
							  		case 13:
											if (moblieInfo.get(attvalue.substring(2,9)) == null)
											return;
										else
											break;
								default:
									return;
							}
						  }
					}
					if(attcode.equals("URLNo")&&classInfo.get(attvalue)!=null){
						
						items[2]=classInfo.get(attvalue);
					}
					context.write(new Text(items[0]),new Text(items[1]+","+items[2]+","+items[3]));
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		 }
	  }
	
	  public static class UserStaticInfoReducer
      					extends Reducer<Text,Text,Text,Text> {
		   @Override
		   public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
				 
				 	int topn = Integer.parseInt(context.getConfiguration().get("topn")); 
				 	String column = context.getConfiguration().get("column");
				 	String[] fields = column.toString().split("#");
				 	HashMap<String,HashMap<String, Long>> map_field = new HashMap<String,HashMap<String, Long>>();
				 	String[] items;
				 	String vkey = key.toString().trim()+",";
				 	
			        for(Text value: values){
			        	 try
			        	 {
				        	 items = value.toString().trim().split(",");
				        	 if(items.length>2)
				        	 {
				        		 HashMap<String,Long> map = map_field.get(items[0]);
				        		 if(map == null)
				        		 {
				        			 map = new HashMap<String,Long>();
					        		 map.put(items[1],Long.parseLong(items[2]));
				        			 map_field.put(items[0], map);
				        		 }
				        		 else
				        		 {
				        			 if(map.get(items[1]) == null)
					        			 map.put(items[1],Long.parseLong(items[2]));
				        			 else
				        				 map.put(items[1],map.get(items[1])+Long.parseLong(items[2]));
				        		 }
				        	 }
			        	 }
			        	 catch(Exception e)
			        	 {;}
			        }
			           
			        for(int i=0; i<fields.length; i++)
			        {
		        		HashMap<String, Long> map = map_field.get(fields[i]);
			        	if(map == null)
			        	{
			        		vkey+=",";
			        	}
			        	else
			        	{
			        		for(int j=0; j<topn; j++)  
			        		{
				        		Iterator<String> it_temp = map.keySet().iterator();
				        		long max = 0;
				        		String s="",skey="";
				        		while(it_temp.hasNext())
				        		{
				        			s = it_temp.next();
				        			if(map.get(s)>max)
				        			{
				        				max = map.get(s);
				        				
				        				skey = s;
				        			}
				        		}
				        		if(map.size()>0 && skey!="")
				        		{
				        			vkey+=skey+"#";
				        			map.remove(skey);
				        		}
				        		else
				        			break;
			        		}  
			        		vkey=vkey.substring(0,vkey.length()-1)+",";
			        	}
			        }
			        context.write(new Text(vkey), new Text("")); 
		     } 
	  }
	
	  public String getCityName(String cityCode,String cityInfo
			  ,Configuration conf) throws IOException{
		  FileSystem fs = FileSystem.get(URI.create(cityInfo),conf); 
		  FSDataInputStream in = null; 
		  Path path = new Path(cityInfo);
		  if(fs.exists(path)) {
		     	in = fs.open(path);
		     	BufferedReader bis = new BufferedReader(new InputStreamReader(in,"GBK")); 
		     	String line = "";
		     	String[] cols;
		     	while ((line = bis.readLine()) != null) {  
		     		 cols=line.split("\\|");
		     		 if(cityCode.equals(cols[1]))
		     			return cols[2];
		     	  }
		   }
		  return "";
	  }
	  
	  public int run(String[] args) throws Exception {   
		    Configuration conf = new Configuration();
		    String cityInfo = "/user/hive/warehouse/aotain_dim.db/to_ref_cityinfo/to_ref_cityinfo.txt";
		    String mobilePath = "/user/hive/warehouse/aotain_dim.db/to_ref_mobile_area/phone_num_area.txt";
		    String classNamePath="/user/hive/warehouse/aotain_dim.db/to_url_class/to_url_class.csv";
		    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		    String[] remainingArgs = optionParser.getRemainingArgs();
		    if (remainingArgs.length != 5) {
		      System.err.println("Usage: UserStaticInfoMR <in> "
		      		+ " <out> <conf> <date> <cityCode>");
		      return 2;
		    }
		    String inputPath = remainingArgs[0];
		    String outPath = remainingArgs[1];
		    String Config = remainingArgs[2];
		    String date = remainingArgs[3];
		    String cityCode = remainingArgs[4];
		    String cityName = getCityName(cityCode,cityInfo,conf);
		    HFileConfigMgr configMgr = new HFileConfigMgr(Config);
		    HfileConfig confHfile = configMgr.config;
			String tableName = confHfile.getTableName();
		    
			String[] topn = confHfile.getKVSplit().split(",");
			String fieldsplit = confHfile.getFieldSplit();
			
			String column = "";
			for(FieldItem item : confHfile.getColumns())
			{
				String text = String.format("%s", item.FieldName);
				column += text +"#"; 
			}
			column  = column.substring(0,column.length() -1 );
			conf.set("topn",topn[0]);
			conf.set("updatecyc",topn[1]);
			conf.set("fieldsplit",fieldsplit);
			conf.set("column",column);
			conf.set("date",date);
			conf.set("cityName",cityName);
			conf.set("mobilePath",mobilePath);
			conf.set("classNamePath",classNamePath );
			System.out.println("-------------topn: " + topn[0]);
      	  	System.out.println("-------------updatecyc: " + topn[1]);
      	  	System.out.println("-------------fieldsplit: " + fieldsplit);
      	  	System.out.println("-------------column: " + column);
      	  	System.out.println("-------------cityName: " + cityName);
      	  	System.out.println("-------------mobilePath: " + mobilePath);
      		System.out.println("-------------classNamePath: " + classNamePath);
			//如果输出目录已存在，需要删除
			FileSystem fsTarget = FileSystem.get(URI.create(outPath),conf);
	        if(fsTarget.exists(new Path(outPath)))
	        {
	      	  	fsTarget.delete(new Path(outPath), true);
	        }
			
		    Job job = Job.getInstance(conf, "UserStaticInfoMR[" + date + "], TableName:"+ tableName );
		    job.setJarByClass(UserStaticInfoMR.class);
		    job.setMapperClass(UserStaticInfoMapper.class);
		    job.setReducerClass(UserStaticInfoReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
	        String[] paths = inputPath.split("#");
	      
	        for(int i=0; i<paths.length; i++)
	        {
	        	//如果输出目录不存在，则不作为输入路径
				FileSystem inputFile = FileSystem.get(URI.create(paths[i]),conf);
	        	if(inputFile.exists(new Path(paths[i])))
	        		FileInputFormat.addInputPath(job,new Path(paths[i]));
	        }
		    FileOutputFormat.setOutputPath(job, new Path(outPath));

		    return job.waitForCompletion(true) ? 0 : 1;
		  }
	  
		  public static void main(String[] args) throws Exception {
			  int res = ToolRunner.run(new UserStaticInfoMR(), args);  
			  System.exit(res);   
		  }
}

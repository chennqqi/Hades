package com.aotain.project.sada;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
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

import com.aotain.common.HFileConfigMgr;
import com.aotain.common.HFileConfigMgr.FieldItem;
import com.aotain.common.HfileConfig;
import com.aotain.project.dmp.PraseCookie.UserMDFMapper;
import com.aotain.project.dmp.PraseCookie.UserMDFReducer;
import com.hadoop.compression.lzo.LzopCodec;


public class LabelDataToHbaseMR extends Configured implements Tool {
	
	
	  public static class UserStaticInfoMapper extends 
	  Mapper<Object, Text, Text, Text>{
		  private Configuration conf;
	
		  private String[] cols;
		  private String cityName = "";
		  private String fieldsplit = "\001";
		  private String column = "";
		  private String date = "";
		  private int updatecyc ;
		  @Override
		  public void setup(Context context) throws IOException,
		  InterruptedException {
		
		    
		}
		 
		  
		  @Override
		  public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
			
				  String[] items = value.toString().split(fieldsplit,-1);
				  String username=items[0];
				  if(username.length()!=40)
					  return;
				  String attcode = items[1];
				  String attvalue = items[2].trim();
				  String freq="1";
				  if(items.length==5)
					  freq=items[3];
				  context.write(new Text(username),new Text(attcode+"|"+attvalue+"|"+freq));
				
			
		 }
	  }
	
	  public static class UserStaticInfoReducer
      					extends Reducer<Text,Text,Text,Text> {
		 int number=0;
		   @Override
		   public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
				    number++;
				 	try{
				    int topn = Integer.parseInt(context.getConfiguration().get("topn")); 
				 	String column = context.getConfiguration().get("column");
				 	String[] fields = column.toString().split("#");
				 	String date= context.getConfiguration().get("date");
	 	            HashMap<String,HashMap<String, Double>> map_field = new HashMap<String,HashMap<String, Double>>();
				 	String[] items;
				 	String vkey = key.toString().trim()+"|";
				 	Pattern p = Pattern.compile("^[0-9\\.]+$", Pattern.CASE_INSENSITIVE);
				
					String match = "";
                   
				
				 	for(Text value: values){
			        
			        	 {
				        	 items = value.toString().trim().split("\\|");
				        		Matcher m = p.matcher(items[2]);
				        		if(!m.find()){
				        			continue;
				        		}
				        	 if(items.length>2)
				        	 {   
				        		 HashMap<String,Double> map = map_field.get(items[0]);
				        		 if(map == null)
				        		 {
				        			 map = new HashMap<String,Double>();
					        		 map.put(items[1],Double.parseDouble(items[2]));
				        			 map_field.put(items[0], map);
				        		 }
				        		 else
				        		 {
				        			 if(map.get(items[1]) == null)
					        			 map.put(items[1],Double.parseDouble(items[2]));
				        			 else
				        				 map.put(items[1],map.get(items[1])+Double.parseDouble(items[2]));
				        		 }
				        	 }
			        	 }
			        	
			        }
			           
			        for(int i=0; i<fields.length; i++)
			        {
		        		HashMap<String, Double> map = map_field.get(fields[i]);
			        	if(map == null)
			        	{
			        		vkey+="|";
			        	}
			        	else
			        	{
			        		for(int j=0; j<topn; j++)  
			        		{
				        		Iterator<String> it_temp = map.keySet().iterator();
				        		double max = 0;
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
			        		vkey=vkey.substring(0,vkey.length()-1)+"|";
			        	}
			        }
			        context.write(new Text(date+"_"+number+"\t"+vkey+date+"|"), new Text("")); 
			        }catch(Exception e){
			        	
			        }
		     } 
	  }
	

	  
	  public int run(String[] args) throws Exception {   
		    Configuration conf = new Configuration();
	
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
		    String topn = remainingArgs[4];
		    conf.set("cloumconfig", Config);
			 String column="";
			  String fileName=conf.get("cloumconfig");
			  FileSystem fs = FileSystem.get(URI.create(fileName),conf); 
			  FSDataInputStream in = null; 
			  BufferedReader fis=null;
		      try {
		    	Path mobilePath = new Path(fileName);
		    	in = fs.open(mobilePath);
		        fis = new BufferedReader(new InputStreamReader(in,"UTF-8"));
//		    	fis = new BufferedReader(new FileReader(fileName));
		        String pattern = null;
//		        int i = 0;
		        String[] cols=null;
		        while ((pattern = fis.readLine()) != null) {
		        	column += pattern +"#";
		         }
		        in.close();
				column  = column.substring(0,column.length() -1 );
				
				conf.set("column",column);
		        System.out.println(column);
				
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
			conf.set("topn",topn);
			conf.set("date",date);
			String jobname = "mylabel"  ;		
			String input = inputPath;
			Job job = Job.getInstance(conf);
			job.setJobName(jobname);
			job.setJarByClass(LabelDataToHbaseMR.class);
			for (String pt : input.split(",")) {
				FileInputFormat.addInputPath(job,new Path(pt));
			}
			job.setNumReduceTasks(1);
			job.setMapperClass(UserStaticInfoMapper.class);
			Path outputpath = new Path(outPath);
			outputpath.getFileSystem(conf).delete(outputpath, true);
			FileOutputFormat.setOutputPath(job, outputpath);
			job.setInputFormatClass(TextInputFormat.class);
			//job.setInputFormatClass(OrcNewInputFormat.class);		
		
			
			job.setReducerClass(UserStaticInfoReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			

		    return job.waitForCompletion(true) ? 0 : 1;
		  }
	  
		  public static void main(String[] args) throws Exception {
			  int res = ToolRunner.run(new LabelDataToHbaseMR(), args);  
			  System.exit(res);   
		  }
}

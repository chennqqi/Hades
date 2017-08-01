package com.aotain.project.sada;

import java.io.IOException; 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserAttReducer extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
		 	 String vkey= "";
		 	 vkey+=key.toString().trim();
			 String[] items;
			 long cnt = 0;
			 long maxdays=0;
			 String maxdate="0";
			boolean flag  = false;
	         for(Text value: values){
	        	 try
	        	 {
		        	 items = value.toString().trim().split(",");
		        	 cnt+=Long.parseLong(items[0]);
		        	 if(Long.parseLong(maxdate)<Long.parseLong(items[2]))
		        		 maxdate = items[2];
		        	 if(maxdays < Long.parseLong(items[1]))
		        		 maxdays = Long.parseLong(items[1]);
		        	 if("0".equals(items[1]))
		        		 flag = true;
	        	 }
	        	 catch(Exception e)
	        	 {;}
	         }
	         if(flag)  maxdays+=1;
	         	
	         
		     vkey+=","+String.valueOf(cnt)+","+maxdays+","+maxdate+",";
		     context.write(new Text(vkey), new Text("")); 
     } 
}
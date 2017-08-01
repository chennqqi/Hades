package com.aotain.dim;

import java.io.IOException; 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TerminalAttReducer extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
		 	 String vkey= "";
			 vkey+=key.toString().trim();
			 String[] items;
			 long cnt = 0;
			 String maxdate="0";
			 
	         for(Text value: values){
	        	 try
	        	 {
		        	 items = value.toString().trim().split(",");
		        	 cnt+=Long.parseLong(items[0]);
		        	 if(Long.parseLong(maxdate)<Long.parseLong(items[1]))
		        		 maxdate = items[1];
	        	 }
	        	 catch(Exception e)
	        	 {;}
	         }      
	         vkey+=","+String.valueOf(cnt)+","+maxdate+",";
	         context.write(new Text(vkey), new Text("")); 
     } 
}
package com.aotain.project.mbanalysis;

import java.io.IOException; 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class PCAnalysisResultMapper extends Mapper<LongWritable,Text,Text,Text>{
	//int index;
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         
		  
		  	
		
		
		  try
		  
		  {
			  String fieldsplit = "\\"+context.getConfiguration().get("fieldsplit");//ÁÐÊôÐÔ·Ö¸ô·û
		      String column = context.getConfiguration().get("column");
	          String[] items=value.toString().split(",",-1);
              String values=items[1]+","+items[2]+","+items[3]+","+items[4];
              context.write(new Text(items[0]), new Text(values));
	          }
     	 catch(Exception e)
     	 {
     		 e.printStackTrace();
     	 
     	 }
      }  
	  
	  private String parseCookie(String cookieValue,Map <String,String>webconfigmap,String[] items,String key){
             
	    	 String webconfigStr=webconfigmap.get(key);
		    if(webconfigStr!=null){
			    	 String[] webconfigs=webconfigStr.split("=",3);
			    	 String [] configparse=webconfigs[1].split("@");
			    	 int position=Integer.parseInt(configparse[2]);
			    	 String startname=configparse[0];
					 String endtname=configparse[1];
					  //index=Integer.parseInt(webconfigs[2]);
					 if(items[position].length()>0&&items[position].indexOf(startname)!=-1){
						  cookieValue=items[position].substring(items[position].indexOf(startname),items[position].length());
						  if(cookieValue.indexOf(endtname)!=-1)
						  cookieValue=cookieValue.substring(0,cookieValue.indexOf(endtname));
						  cookieValue=cookieValue.split("=")[1];
					 }
		    	 }
		  return cookieValue;
	  }
	 
	  public static void main(String[] args) {
	      String ss="1";
	      System.out.println(ss.length());
		// String s="pgv_info=ssid=s6263183172";
		// String s2=s.split("=")[1];
		 //System.out.println(s2);
		String s="ttgg=oopp;cna=225;cna=000";
		String cookieValue=s.substring(s.indexOf("cna"),s.length());
		System.out.println(cookieValue);
		if(cookieValue.indexOf(";")!=-1)
		cookieValue=cookieValue.substring(0,cookieValue.indexOf(";"));
		 cookieValue=cookieValue.split("=")[1];
		 System.out.println(cookieValue);
	 }
}

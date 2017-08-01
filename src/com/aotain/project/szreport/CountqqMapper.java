package com.aotain.project.szreport;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class CountqqMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
	
		  try { 
				 String fieldsplit = "\\|";//ÁÐÊôÐÔ·Ö¸ô·û
		         String[] items=value.toString().split(fieldsplit,-1);
		         String domain=items[3];
		   	     String systemour=items[6].toLowerCase().trim();  
		         boolean sysflag=systemour.equals("windows")||systemour.equals("macintosh")||systemour.equals("x11")||systemour.equals("linux");
			  if(sysflag){
				 
				   String atime=items[11];
				 if(domain.contains("qq.com")){
						 String cookie=items[12];
						 if(cookie.indexOf("o_cookie")!=-1){
							 String cookieValue=cookie.substring(cookie.indexOf("o_cookie"),cookie.length());
							 if(cookieValue.indexOf(";")!=-1)
									cookieValue=cookieValue.substring(0,cookieValue.indexOf(";"));
							        if(cookieValue.split("=").length==2){
										cookieValue=cookieValue.split("=")[1];
										cookieValue=CommonFunction.getQQNumber(cookieValue);
									}else
										cookieValue=null;
									if(cookieValue!=null)
									context.write(new Text(items[1]),new Text("shenzhen,"+cookieValue+","+atime));
						 }
					 }
			 }
		  }catch (Exception e) {
				
		  }
	};
}

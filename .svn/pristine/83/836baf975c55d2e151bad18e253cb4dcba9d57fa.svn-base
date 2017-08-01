package com.aotain.project.mbanalysis;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class MBAnalysisMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		 String fieldsplit = "\\"+context.getConfiguration().get("fieldsplit");//ÁÐÊôÐÔ·Ö¸ô·û
		 
		  String column = context.getConfiguration().get("column");
         
		  String[] items=value.toString().split(fieldsplit,-1);
		  try {   
			  String systemour=items[1].toLowerCase().trim();
		      boolean sysflag=systemour.equals("android")||systemour.equals("ios");
		      String sys = items[1]+","+items[2]+","+items[5];
			  if(sysflag){
				  //1 opersys //2opersysver //5 device
				  boolean flag = StringUtils.isNotEmpty(items[1])&&StringUtils.isNotEmpty(items[2])&&StringUtils.isNotEmpty(items[5]);
				  boolean sysverflag = filterSysver(items[1],items[2]);
				  if(flag && sysverflag){
					  if(!sysverflag){
						  sys = items[1];  
					  }
					  context.write(new Text(items[0]), new Text(sys)); 
				  }
			  }
		  }catch (Exception e) {
			  
		  }
	};
	
	
	public  boolean filterSysver(String sys,String version){
		String regEx = "";
		if(sys.toLowerCase().equals("android")){
			regEx = "[2-5]\\.[0-9]\\.[0-9]|[2-5]\\.[0-9]";
		}else if(sys.toLowerCase().equals("ios")){
			regEx = "[2-8]\\.[0-9]\\.[0-9]|[2-8]\\.[0-9]";
		}
		
		String ret = CommonFunction.findByRegex(version, regEx, 0);
		if(ret == null){
			return false;
		}
		return true;
	}
	
	public static void main(String[] args) {
		String systemour="".toLowerCase().trim();
	}
}

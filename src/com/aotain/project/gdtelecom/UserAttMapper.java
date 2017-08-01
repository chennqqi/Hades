package com.aotain.project.gdtelecom;

import com.aotain.common.CommonFunction;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAttMapper extends Mapper<LongWritable, Text, Text, Text>
{
  public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    String date = context.getConfiguration().get("date");
    long limitday = (context.getConfiguration().get("limitday").equals("null")) ? 0L : Long.parseLong(context.getConfiguration().get("limitday"));
    String rowkey = context.getConfiguration().get("rowkey");
    String regexp = "";

    String vret = value.toString();
    try
    {
      String[] items = vret.split(",", -1);
      regexp = rowkey.split("=")[2];
      if (((!regexp.equals("null")) && (!validateUser(items[0]))) || (items[0] ==null || items[0].length()==0)) {
        return;
      }

      

      if (items.length == 6) {
 	  	 // 临时处理iphone和其它 
   		String attvalue=items[2];
   		/*if(attvalue.contains("其它 ")){
   			attvalue = attvalue.replaceAll("其它", "其他");
   		}
   		else if(attvalue.toLowerCase().contains("iphone")){
   			attvalue = attvalue.toUpperCase();
   		}
  		String att = attvalue.replaceAll("[\\s]", "");// 去空格
  		if(att.contains("IPHONE8")){
  			attvalue = attvalue.replaceAll("苹果.*", "苹果#其他");
  		}*/
    	  
        if (limitday == 0L) {
          context.write(
            new Text(items[0] + "," + items[1] + "," + 
            		attvalue), new Text(items[3] + ",0," + items[4]));
        } else {
          SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
          Date date1 = format.parse(items[4].substring(0, 8));
          Date date2 = format.parse(date);
          long day1 = date1.getTime();
          long day2 = date2.getTime();
          long days = (day2 - day1) / 1000L / 3600L / 24L;

          if (days < limitday)
            context.write(
              new Text(items[0] + "," + items[1] + "," + 
            		  attvalue), 
              new Text(items[3] + ",0," + items[4]));
        }
      }
      else if (items.length == 7){
    	  
    	  	 // 临时处理iphone和其它 
      		String attvalue=items[2];
      		/*if(attvalue.contains("其它 ")){
      			attvalue = attvalue.replaceAll("其它", "其他");
      		}
      		else if(attvalue.toLowerCase().contains("iphone")){
      			attvalue = attvalue.toUpperCase();
      		}
      		String att = attvalue.replaceAll("[\\s]", "");// 去空格
      		if(att.contains("IPHONE8")){
      			attvalue = attvalue.replaceAll("苹果.*", "苹果#其他");
      		}*/
    	  
	        if (limitday == 0L) {
	          context.write(
	            new Text(items[0] + "," + items[1] + "," + 
	            		attvalue), new Text(items[3] + "," + items[4] + "," + items[5]));
	        }
	        else {
	          SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	          Date date1 = format.parse(items[5].substring(0, 8));
	          Date date2 = format.parse(date);
	          long day1 = date1.getTime();
	          long day2 = date2.getTime();
	          long days = (day2 - day1) / 1000L / 3600L / 24L;
	
	          if (days < limitday)
	            context.write(
	              new Text(items[0] + "," + items[1] + "," + 
	            		  attvalue), 
	              new Text(items[3] + "," + items[4] + "," + items[5]));
	        }
      }
    }
    catch (Exception Exception)
    {
    }
  }

  private static boolean validateUser(String username)
  {
    boolean ret = false;

    String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
    if (aa == null)
    {
      if (username.indexOf("IPCYW") == 0)
      {
        ret = true;
      }

    }
    else {
      ret = true;
    }

    return ret;
  }
}
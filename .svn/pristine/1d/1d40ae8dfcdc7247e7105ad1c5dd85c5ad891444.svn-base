package com.aotain.ods;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;

public class HTTPFilterMapper extends Mapper<LongWritable,Text,Text,Text>{

	public void map(LongWritable key,Text value,Context context){

		 String strKey = "";
		 //获取到需要过滤的url
		 String strDomain = context.getConfiguration().get("app.domains");
		 String strCurDate = context.getConfiguration().get("app.date");
		 String[] arrDomain = strDomain.split("\\|");
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 /**新需求修改后字段结构 2014-12-24 turk
		  *0 AreaID 
		   1 UserName
		   2 SrcIP
		   3 Domain
		   4 Url
		   5 Refer
		   6 OperSys
		   7 OperSysVer
		   8 Browser
		   9 BrowserVer
		   10 Device
		   11 AccessTime
		   12 Cookie
		   13 Keyword
		   14 UrlClassID
		   15 referdomain
		   16 referclassid
		  */
		 if(items.length != 17)
			 return;
		 
		 /*for(String domain : arrDomain)
		 {
			 if(items[3].equals(domain))
			 {
				 return;
			 }
		 }*/
		 
		 //验证用户账号的合法性
		 if(!validateUser(items[1]))
			 return;
		 
		 String accesstime = CommonFunction.findByRegex(items[11], "[0-9]*", 0);
		 if(accesstime==null)
			 return;
		 
		 if(items[11].trim().isEmpty())
			 return;
		 
		 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		 Date dStartTime = new Date(Long.parseLong(items[11])*1000L);
		 String strDate = df.format(dStartTime);
		 
		 try {
			Date curDate = df.parse(strCurDate);
			Date pre2Date = new Date(curDate.getTime() - 24*3600*1000L);
			Date nextDate = new Date(curDate.getTime() + 24*3600*1000L);
			
			if(dStartTime.getTime() < pre2Date.getTime() || dStartTime.getTime() >= nextDate.getTime())
				return;
			
		 } catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			
		 }
		 
		 if(strDate.equals(strCurDate))
		 {
			 strKey = String.format("TODAY|%s",items[1]);
		 }
		 else
		 {
			 strKey = String.format("%s|%s",strDate,items[1]);
		 }
		 String strValue = value.toString();
	     try {			
				context.write(new Text(strKey), new Text(strValue));
			 } catch (IOException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 } catch (InterruptedException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
			
	}
	
	private boolean validateUser(String username)
	{
		boolean ret = false;
		
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			aa = CommonFunction.findByRegex(username, "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", 0);
			if(aa == null)
				ret = false;
			else
				ret = true;
		}
		else
		{
			ret = true;
		}
		
		return ret;
	}
}

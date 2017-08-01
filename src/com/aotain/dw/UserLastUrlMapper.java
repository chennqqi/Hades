package com.aotain.dw;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;


/**
 * 用户一次访问行为的最后一条url
 * @author Administrator
 *
 */
public class UserLastUrlMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context) throws IOException{

		 String strKey = "";
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
		 
		 String accesstime = CommonFunction.findByRegex(items[11], "[0-9]*", 0);
		 if(accesstime == null || accesstime.trim().isEmpty())
			 return;
		 SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmm");
		 Date dStartTime = new Date(Long.parseLong(accesstime)*1000L);
		 String strStartTime = df.format(dStartTime);
		 strKey = String.format("%s_%s",items[1],strStartTime);
		
	     try {			
				context.write(new Text(strKey), new Text(value));
			
			 } catch (InterruptedException e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
			
	}
}

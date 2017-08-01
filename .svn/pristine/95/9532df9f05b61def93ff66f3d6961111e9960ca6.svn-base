package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;

public class DomainStatMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
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
		 
		 if(!items[10].trim().isEmpty() 
				 || items[6].trim().equals("Android"))
			 return;//过滤手机端的数据
		
		 try {		
			 //全域名
			 String domain = items[3];
			 String[] strArr = domain.split("\\.",-1);
			 String rexp = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
			 String IP = CommonFunction.findByRegex(domain, rexp, 0);
			 
			 if(strArr.length >= 2)
			 {//根域名
				 //判断是否为IP地址
				 if(IP != null)
				 {
					 strKey = IP+"_1";
				 }
				 else if(IsSpecialRoot(domain) && strArr.length >= 3)
				 {
					 strKey = String.format("%s.%s.%s_1", strArr[strArr.length-3],
							 strArr[strArr.length-2],strArr[strArr.length-1]);
				 }
				 else
				 {
					 strKey = String.format("%s.%s_1", 
							 strArr[strArr.length-2],strArr[strArr.length-1]);
				 }
				 context.write(new Text(strKey), new IntWritable(1));
			 }
			 
			 if(strArr.length >= 3 && !domain.contains("www.") && IP == null)
			 {
				 //二级
				 //排除www
				 //判断是否为IP地址
				 
				 if(IsSpecialRoot(domain) && strArr.length == 3)
					 return;//对于com.cn的域名特殊判断
				 
				 if(IsSpecialRoot(domain) && strArr.length > 3)
				 {
					 strKey = String.format("%s.%s.%s.%s_2", 
							 strArr[strArr.length-4],
							 strArr[strArr.length-3],
							 strArr[strArr.length-2],strArr[strArr.length-1]);
				 }
				 else
				 {
					 strKey = String.format("%s.%s.%s_2", strArr[strArr.length-3],
						 strArr[strArr.length-2],strArr[strArr.length-1]);
				 }
				 context.write(new Text(strKey), new IntWritable(1));
			 }
			 if(strArr.length > 3)
			 {//全域名
				 strKey = domain + "_3";
				 context.write(new Text(strKey), new IntWritable(1));
			 }
			 
			 //
		 } catch (InterruptedException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
		 }
			
	 }
	 
	 
	 private boolean IsSpecialRoot(String domain)
	 {
		 boolean blReturn = false;
		 if(domain.contains(".com.")
				 || domain.contains(".co.")
				 || domain.contains("gd.cn")
				 || domain.contains("cn.com")
				 || domain.contains(".gov.")
				 || domain.contains(".net.")
				 || domain.contains(".edu.")
				 || domain.contains(".org.")
				 || domain.contains(".ac."))
			 blReturn = true;
		 return blReturn;
	 }

}

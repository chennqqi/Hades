package com.aotain.ods;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.datanucleus.util.StringUtils;

import com.aotain.common.CommonFunction;

public class MailFileMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
	 private static boolean validateUser(String username){
		boolean ret = false;
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			ret = true;
		}
		
		return ret;
	}
	 
	 public static String findByRegex(String str, String regEx, int group)
	 	{
	 		String resultValue = null;
	 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
	 			return resultValue;
	 		
	 		
	 		Pattern p = Pattern.compile(regEx);
	 		Matcher m = p.matcher(str);

	 		boolean result = m.find();
	 		if (result)
	 		{
	 			resultValue = m.group(group);
	 		}
	 		return resultValue;
	 	}
	 
	 
	public void map(LongWritable key,Text value,Context context) 
			throws IOException{
		String sUserName = "";
	
		String[] items = value.toString().split("\\|",-1);
		if(items.length==14)
		{
			String cell = items[0];
			String temp = null;
			try
			{
				if(!validateUser(cell))
				{
					sUserName = cell;
					cell=items[13];
					if(cell.length()>0)
					{
						String domains[] = items[6].trim().split("\\.");
						int domainLength = domains.length;
						String rootDomain = null;
						if (domainLength > 1) {
							String level2= domains[domainLength - 2];		
							if("com".equals(level2) && domainLength > 2){
							 rootDomain = domains[domainLength - 3]+"."+domains[domainLength - 2] + "."
									+ domains[domainLength - 1];
							 }else{
								 rootDomain =domains[domainLength - 2] + "."
											+ domains[domainLength - 1]; 
							 }
						}
						rootDomain = rootDomain != null ? rootDomain : "null.com";
						//mail
						temp = findByRegex(cell, "([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)", 0);
						if(temp != null)
						{
							context.write(new Text("1|"+sUserName+"|"+temp), new Text(rootDomain));
						}
						
						cell =cell.trim().toLowerCase();
						if (cell.contains("%")){
							cell = cell.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
							cell = java.net.URLDecoder.decode(cell, "utf-8");
						}
						cell = cell.replace("\"", "");
						
						//imei
						temp = findByRegex(cell, "imei(=|:|_)(([1-9]{1})\\d{13,14})[;&_,}\\s*]{1}", 2);
						if(temp != null)
						{
							context.write(new Text("2|"+sUserName+"|"+temp), new Text(rootDomain));
						}
						
						//mac
						temp = findByRegex(cell, "(mac|macaddress)(=|:|_)([0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2})[;&_,}\\s*]{1}", 3);
						if(temp != null)
						{
							temp=temp.replace("-", ":").toUpperCase();
							context.write(new Text("3|"+sUserName+"|"+temp), new Text(rootDomain));
						}
					
						//idfa
						temp = findByRegex(cell, "(idfa|idfv)(=|:)(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}", 3);
						if(temp != null)
						{
							temp=temp.toUpperCase();
							context.write(new Text("4|"+sUserName+"|"+temp), new Text(rootDomain));
						}
					}
				}
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
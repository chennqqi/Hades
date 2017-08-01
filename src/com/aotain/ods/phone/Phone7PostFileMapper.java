package com.aotain.ods.phone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class Phone7PostFileMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
	private static Map<String,String> map=new HashMap<String,String>();
	 
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

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("map");
		  map  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
	}
	 
	 
	 
	public void map(LongWritable key,Text value,Context context) 
			throws IOException, InterruptedException{
		String sUserName = "";
		String phone=null;
		Map<String,String> notkmap=new HashMap<String,String>();
		Map<String,List<String>> fields=new HashMap<String,List<String>>();
		List<String> list1 = new ArrayList<String>();
		List<String> list2 = new ArrayList<String>();
		
		try
      {
			String notnk = context.getConfiguration().get("notnk");
			String[] splits = notnk.split(",", -1);
			for (String split : splits) {
				if (split.trim().length() == 3) {
					notkmap.put(split, split);
				}
			}

			String[] items = value.toString().split("\\|", -1);
			if (items.length == 14) {
				String cell = items[0];

				if (!validateUser(cell)) {
					sUserName = cell;
					String url = items[7].trim().toLowerCase();
					if (url.contains("%")){
						url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						url = java.net.URLDecoder.decode(url, "utf-8");
					}
					if(url.length() >= 11)  list1.add(url.replace("\"", ""));

					String cookie = items[10].trim().toLowerCase();;
					if (cookie.contains("%")){
						cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						cookie = java.net.URLDecoder.decode(cookie, "utf-8");
					}
					if(cookie.length() >= 11)  list2.add(cookie.replace("\"", ""));

					String postcont = items[13].trim().toLowerCase();
					if (postcont.contains("%")){
						postcont = postcont.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						postcont = java.net.URLDecoder.decode(postcont, "utf-8");
					}
					if(postcont.length() >= 11)  list1.add(postcont.replace("\"", ""));
							
					fields.put("1", list1);
					fields.put("2", list2);
					
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
					
					
					for (Map.Entry<String, List<String>> entry : fields.entrySet()) {
						 String k = entry.getKey();
						 List<String> v = entry.getValue();
						 for (String temp : v) {
									phone =  findByRegex(temp, "(=|:)([1][0-9]{10})[;&,}\\s*]{1}", 2);
									if(phone != null)
									{
										String p3 = phone.substring(0, 3);
										String p7 = phone.substring(0, 7);
										if (map.get(p7) != null) {
											context.write(
													new Text(sUserName + "|" + phone),
													new Text(k + "|" + rootDomain));
										} else if (notkmap.get(p3) != null) {
											context.write(
													new Text(sUserName + "|" + phone),
													new Text(k + "|" + rootDomain));
										}
									}
								}
							
						 }
						
					}
				}

		}
		catch (Exception e)  {;}
	}
}
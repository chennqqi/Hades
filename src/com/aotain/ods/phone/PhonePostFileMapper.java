package com.aotain.ods.phone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class PhonePostFileMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
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

			Map<String, Integer> rules = new HashMap<String, Integer>();
			rules.put("phone=", 6);
			rules.put("mobile=", 7);
			rules.put("name=", 5);
			rules.put("tel=", 4);
			rules.put("account=", 8);
			rules.put("mobileno=", 9);
			rules.put("phonenumber=", 12);
			String reg = "^\\d+$";
			String[] items = value.toString().split("\\|", -1);
			if (items.length == 14) {
				String cell = items[0];

				if (!validateUser(cell)) {
					sUserName = cell;
					String url = items[7];
					String urllow = url.trim().toLowerCase();
					if (urllow.contains("%")){
						urllow = urllow.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						urllow = java.net.URLDecoder.decode(urllow, "utf-8");
					}
					if(urllow.length() >= 11)  list1.add(urllow);

					String cookie = items[10];
					String cookielow = cookie.trim().toLowerCase();
					if (cookielow.contains("%")){
						cookielow = cookielow.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						cookielow = java.net.URLDecoder.decode(cookielow,
								"utf-8");
					}
					if(cookielow.length() >= 11)  list2.add(cookielow);

					String postcont = items[13];
					String postcontlow = postcont.trim().toLowerCase();
					if (postcontlow.contains("%")){
						postcontlow = postcontlow.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						postcontlow = java.net.URLDecoder.decode(postcontlow,
								"utf-8");
					}
					if(postcontlow.length() >= 11)  list1.add(postcontlow);

					fields.put("1", list1);
					fields.put("3", list2);
					
					for (Map.Entry<String, List<String>> en : fields.entrySet()) {
						 String k = en.getKey();
						 List<String> v = en.getValue();
							for (String temp : v) {
								Iterator<Map.Entry<String, Integer>> it = rules
										.entrySet().iterator();
								while (it.hasNext()) {
									Map.Entry<String, Integer> entry = it.next();
									if (temp.contains(entry.getKey())) {
										int index = temp.indexOf(entry.getKey());
										int pos = entry.getValue();
										
										if(temp.length() < index + pos + 11) break;
										phone = temp.substring(index + pos,
												index + pos + 11).trim();

										if (phone.length() == 11
												&& phone.matches(reg)) {
											String p3 = phone.substring(0, 3);
											String p7 = phone.substring(0, 7);
											if (map.get(p7) != null) {
												context.write(new Text(sUserName
														+ "|" + phone), new Text(
														k));
											} else if (notkmap.get(p3) != null) {
												context.write(new Text(sUserName
														+ "|" + phone), new Text(
														k));
											}
										}
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
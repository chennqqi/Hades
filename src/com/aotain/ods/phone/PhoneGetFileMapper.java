package com.aotain.ods.phone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class PhoneGetFileMapper extends Mapper<NullWritable,OrcStruct,Text,Text>{
	 
	final static String inputSchema = "struct<areaid:string,username:string,srcip:string,domain:string,"
			+ "url:string,refer:string,opersys:string,opersysver:string,browser:string,"
			+ "browserver:string,device:string,accesstime:bigint,cookie:string,keyword:string,"
			+ "urlclassid:string,referdomain:string,referclassid:string,ua:string,"
			+ "destinationip:string,sourceport:string,destinationport:string>";
	static StructObjectInspector inputOI;
	private static Map<String,String> map=new HashMap<String,String>();
	
	
	public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			TypeInfo tfin = TypeInfoUtils
					.getTypeInfoFromTypeString(inputSchema);
			inputOI = (StructObjectInspector) OrcStruct
					.createObjectInspector(tfin);
			String str = context.getConfiguration().get("map");
			 map  = ( HashMap<String,String>) ObjectSerializer
			            .deserialize(str);
					
		}
	 
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
	 
	 
	public void map(NullWritable key, OrcStruct value, Context context) 
			throws IOException, InterruptedException{
		String sUserName = "";
		String phone=null;
		Map<String,String> notkmap=new HashMap<String,String>();
		Map<String,List<String>> fields=new HashMap<String,List<String>>();
		List<String> list1 = new ArrayList<String>();
		List<String> list2 = new ArrayList<String>();
		
		try
      {
			  List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
			  if( ilst.size() != 21){
				  return;
			  }
			  
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
			String cell =ilst.get(1).toString();
			if (!validateUser(cell)) {
				sUserName = cell;
				String url = ilst.get(4).toString();
				String urllow = url.trim().toLowerCase();
				if (urllow.contains("%")){
					urllow = urllow.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					urllow = java.net.URLDecoder.decode(urllow, "utf-8");
				}
				if(urllow.length() >= 11)  list1.add(urllow) ;
				
				String cookie = ilst.get(12).toString();
				String cookielow = cookie.trim().toLowerCase();
				if (cookielow.contains("%")){
					cookielow = cookielow.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					cookielow = java.net.URLDecoder.decode(cookielow,
							"utf-8");
				}
				if(cookielow.length() >= 11)  list2.add(cookielow);
		
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
		catch (Exception e)  {;}
	}
}
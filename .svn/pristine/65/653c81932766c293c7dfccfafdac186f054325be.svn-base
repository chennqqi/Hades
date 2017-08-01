package com.aotain.ods.phone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class Phone7GetFileMapper extends Mapper<NullWritable,OrcStruct,Text,Text>{
	 
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
			
			String cell =ilst.get(1).toString();
			if (!validateUser(cell)) {
				sUserName = cell;
				String url = ilst.get(4).toString().trim().toLowerCase();
				if (url.contains("%")){
					url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					url = java.net.URLDecoder.decode(url, "utf-8");
				}
				if(url.length() >= 11)  list1.add(url.replace("\"", ""));
				fields.put("1", list1);
				
				String cookie = ilst.get(12).toString().trim().toLowerCase();
				if (cookie.contains("%")){
					cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					cookie = java.net.URLDecoder.decode(cookie, "utf-8");
				}
				if(cookie.length() >= 11)  list2.add(cookie.replace("\"", ""));
				fields.put("2", list2);
				
				String domains[] = ilst.get(3).toString().trim().split("\\.");
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
		catch (Exception e)  {;}
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
	 
}
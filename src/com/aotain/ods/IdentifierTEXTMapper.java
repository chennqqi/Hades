package com.aotain.ods;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class IdentifierTEXTMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
	private static Map<String,String> map=new HashMap<String,String>();
	 
	 private static boolean validateUser(String username){
		boolean ret = false;
		
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			if(username.indexOf("IPCYW")==0)
			{		
				ret = true;
			}
		}
		else
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

	 private void IdentifierSub(Context context, String sUserName,
				Map<String, String> notkmap, Map<String, String> app,
				String domain, String rootDomain, int k, String tp,boolean flag)
				throws IOException, InterruptedException {
			String temp;
			Text outv = new Text(k + "|" + rootDomain);

			//mail
			temp = findByRegex(tp, "mail(=|:)([\\w[.-]]+@[\\w[.-]]+\\.[\\w]+)[;&,}\\s*]{1}", 2);
			if(temp != null  && temp.length() <= 35 && !temp.endsWith(".com.cn")
					&& (temp.endsWith(".com") || temp.endsWith(".cn")))
			{
				context.write(new Text("1|"+sUserName+"|"+temp), outv);
			}
			
			//imei
			temp = findByRegex(tp, "imei(=|:|_)(([1-9]{1})\\d{13,14})[;&_,}\\s*]{1}", 2);
			if(temp != null)
			{
				context.write(new Text("2|"+sUserName+"|"+temp), outv);
			}
			
			//mac
			temp = findByRegex(tp, "(mac|macaddress)(=|:|_)([0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2}(:|-)[0-9a-z]{2})[;&_,}\\s*]{1}", 3);
			if(temp != null)
			{
				temp=temp.replace("-", ":").toUpperCase();
				if(!"02:00:00:00:00:00".equals(temp)){
					context.write(new Text("3|"+sUserName+"|"+temp), outv);
				}
			}

			//idfa
			temp = findByRegex(tp, "(idfa|idfv)(=|:)(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}", 3);
			if(temp != null)
			{
				temp=temp.toUpperCase();
				context.write(new Text("4|"+sUserName+"|"+temp), outv);
			}else{
				if(flag){
					String lishj = app.get(domain);
					if(lishj != null){
						String[] lishjs = lishj.split("::");
						temp = findByRegex(tp, lishjs[1], Integer.parseInt(lishjs[0]));
						if(temp != null)
						{
							temp=temp.toUpperCase();
							context.write(new Text("4|"+sUserName+"|"+temp), outv);
						}
					}
				}
			}
			
			//phone
			temp =  findByRegex(tp, "(=|:)([1][0-9]{10})[;&,}\\s*]{1}", 2);
			if(temp != null)
			{
				String p3 = temp.substring(0, 3);
				String p7 = temp.substring(0, 7);
				if (map.get(p7) != null  || notkmap.get(p3) != null) {
						context.write(new Text("5|"+sUserName+"|"+temp), outv);
				}
			}
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
		Map<String,String> notkmap=new HashMap<String,String>();
		
		try
      {
			String notnk = context.getConfiguration().get("notnk");
			String[] splits = notnk.split(",", -1);
			for (String split : splits) {
				if (split.trim().length() == 3) {
					notkmap.put(split, split);
				}
			}

			 Map<String,String> app=new HashMap<String,String>();
			 app.put("kepler.jd.com", "1::deviceid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("market.m.sjzhushou.com", "1::ifa:(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("yktd.m.cn.miaozhen.com", "2::(m0|m5)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("vyk.admaster.com.cn", "2::(o|z)(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("m.game.weibo.cn", "1::deviceid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("ad.ximalaya.com", "2::(adid|udid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("54.222.190.235", "1::tdid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("capi.douyucdn.cn", "1::devid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("ark.letv.com", "1::did=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("passport.iqiyi.com", "1::device_id=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("paopao.iqiyi.com", "1::m_device_id=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("search.video.qiyi.com", "1::u=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("subscription.iqiyi.com", "1::ckuid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("iface.iqiyi.com", "2::(qyid|cupid_uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("iface2.iqiyi.com", "2::(qyid|cupid_uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("msg.71.am", "2::(u|uid)=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("api.yuedu.iqiyi.com", "1::qiyiid=(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 app.put("mi.gdt.qq.com", "1::m5:(([a-z0-9]{8})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{4})-([a-z0-9]{12}))[;&,}\\s*]{1}");
			 
			String[] items = value.toString().split("\\|", -1);
			if (items.length == 14) {
				String cell = items[0];
				if (!validateUser(cell)) {
					return;
				}

				sUserName = cell;
				
				String url = items[7].trim().toLowerCase();
				if (url.contains("%")){
					url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					url = java.net.URLDecoder.decode(url, "utf-8");
				}
				url = url.replace("\"", "").toLowerCase();
				
				String cookie = items[10].trim().toLowerCase();
				if (cookie.contains("%")){
					cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					cookie = java.net.URLDecoder.decode(cookie, "utf-8");
				}
				cookie = cookie.replace("\"", "").toLowerCase();
				
				String postcont = items[13].trim().toLowerCase();
				if (postcont.contains("%")){
					postcont = postcont.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					postcont = java.net.URLDecoder.decode(postcont, "utf-8");
				}
				postcont = postcont.replace("\"", "").toLowerCase();
				
				String domain = items[6].trim();
				String domains[] = domain.split("\\.");
				int domainLength = domains.length;
				String rootDomain = null;
				if(domainLength > 2) {
						 rootDomain = domain.substring(domain.indexOf(".")+1); 
				}else{
					     rootDomain = domain;
				}
				rootDomain = rootDomain != null ? rootDomain : "null.com";
				
				String timestamp = items[11].trim();
				long createtime = 10L;
				try {
					createtime =Long.parseLong(timestamp) ;
				}
				catch (Exception e)  {;}
				SimpleDateFormat sdf = new SimpleDateFormat("HH");
				long hour =Long.parseLong(sdf.format(createtime*1000L));
				
				if((hour >= 21 && hour <= 23) || (hour >= 0 && hour <= 7)){
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								url,true);
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 3,
								cookie,false);
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								postcont,false);
				}else{
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 1,
								url,true);
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								cookie,false);
					 IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 1,
								postcont,false);
				}
		
			
				}

		}
		catch (Exception e)  {;}
		
	}
    
}
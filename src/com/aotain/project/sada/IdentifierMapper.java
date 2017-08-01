package com.aotain.project.sada;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.ObjectSerializer;

public class IdentifierMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
	private static Map<String,String> map=new HashMap<String,String>();
	 
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
		Map<String,String> notkmap=new HashMap<String,String>();
		
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
		 
		try
      {
			String notnk = context.getConfiguration().get("notnk");
			String[] splits = notnk.split(",", -1);
			for (String split : splits) {
				if (split.trim().length() == 3) {
					notkmap.put(split, split);
				}
			}

			String[] items = value.toString().split("\t", -1);
//			if (items.length == 9) {
				String cell = items[1];

				if (StringUtils.isNotEmpty(cell) && !cell.equals("none")) {
					sUserName = cell;
					
					String url = items[3].trim();
					URL sl = new URL(url);
					String domain = sl.getHost();
					String domains[] = domain.split("\\.");
					int domainLength = domains.length;
					String rootDomain = null;
					if(domainLength > 2) {
							 rootDomain = domain.substring(domain.indexOf(".")+1); 
					}else{
						     rootDomain = domain;
					}
					rootDomain = rootDomain != null ? rootDomain : "null.com";
					
					String timestamp = items[2].trim();
					long createtime = 10L;
					try {
						createtime =Long.parseLong(timestamp) ;
					}
					catch (Exception e)  {;}
					SimpleDateFormat sdf = new SimpleDateFormat("HH");
					long hour =Long.parseLong(sdf.format(createtime));
					
					if (url.contains("%")){
						url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						url = java.net.URLDecoder.decode(url, "utf-8");
					}
					
					String cookie = new String(decodeX(items[7].trim()));
					if (cookie.contains("%")){
						cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
						cookie = java.net.URLDecoder.decode(cookie, "utf-8");
					}
					if((hour >= 21 && hour <= 23) || (hour >= 0 && hour <= 7)){
						 IdentifierSub(context, sUserName, notkmap,
									app, domain, rootDomain, 2,
									url.replace("\"", "").toLowerCase(),true);
						 IdentifierSub(context, sUserName, notkmap,
									app, domain, rootDomain, 3,
									cookie.replace("\"", "").toLowerCase(),false);
					}else{
						 IdentifierSub(context, sUserName, notkmap,
									app, domain, rootDomain, 1,
									url.replace("\"", "").toLowerCase(),true);
						 IdentifierSub(context, sUserName, notkmap,
									app, domain, rootDomain, 2,
									cookie.replace("\"", "").toLowerCase(),false);
					}
					
					}
//				}

		}
		catch (Exception e)  {;}
	}

	private void IdentifierSub(Context context, String sUserName,
			Map<String, String> notkmap, Map<String, String> app,
			String domain, String rootDomain, int k, String tp,boolean flag)
			throws IOException, InterruptedException {
		String temp;
		Text outv = new Text(k + "|" + rootDomain);
		//mail
//		temp = findByRegex(tp, "([a-zA-Z0-9_\\-\\.]{1,50})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)", 0);
//		if(temp != null)
//		{
//			context.write(new Text("1|"+sUserName+"|"+temp), outv);
//		}
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
			context.write(new Text("3|"+sUserName+"|"+temp), outv);
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
	
	 /** 
     * 编码 
     * @param bstr 
     * @return String 
     */  
    public static String encodeX(byte[] bstr){  
    return new sun.misc.BASE64Encoder().encode(bstr);  
    }  
  
    /** 
     * 解码 
     * @param str 
     * @return string 
     */  
    public static byte[] decodeX(String str){  
    byte[] bt = null;  
    try {  
        sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();  
        bt = decoder.decodeBuffer( str );  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
  
        return bt;  
    }  
    

    
}
package com.aotain.project.gdtelecom;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.ObjectSerializer;

public class IdentifierPMapper extends Mapper<NullWritable,OrcStruct,Text,Text>{
	 
	private static final String inputSchema = "struct<url:string,useragent:string,destinationip:string,destinationport:string,"
			+ "sourceport:string,link_info:string,cookie:string,contenttype:string,account_nbr:string,protocol_type:string,"
			+ "sourceip:string,domain:string,visit_time:string,pack_len:string,pack_content:string,is_ip:int,content_type:string>";
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
			  List<Object> ilst = inputOI.getStructFieldsDataAsList(value);
			
			  if( ilst.size() < 15){
				  return;
			  }
			  
			String notnk = context.getConfiguration().get("notnk");
			String[] splits = notnk.split(",", -1);
			for (String split : splits) {
				if (split.trim().length() == 3) {
					notkmap.put(split, split);
				}
			}
			
			String cell =ilst.get(8).toString().trim();
			if (cell !=null && cell.length() != 0) {
				sUserName = cell;
				String url = ilst.get(0).toString().trim();
				if (url.contains("%")){
					url = url.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					url = java.net.URLDecoder.decode(url, "utf-8");
				}
				url = url.replace("\"", "").toLowerCase();
				
				String cookie = ilst.get(6).toString().trim();
				if (cookie.contains("%")){
					cookie = cookie.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					cookie = java.net.URLDecoder.decode(cookie, "utf-8");
				}
				cookie = cookie.replace("\"", "").toLowerCase();
				
				String pack_contnt = new String(decode(ilst.get(14).toString().trim()));
				if (pack_contnt.contains("%")){
					pack_contnt = pack_contnt.replaceAll("%(?![0-9a-fA-F]{2})", "%25");  
					pack_contnt = java.net.URLDecoder.decode(pack_contnt, "utf-8");
				}
				pack_contnt =pack_contnt.replace("\"", "").toLowerCase();
				
				String domain = ilst.get(11).toString().trim();
				String domains[] = domain.split("\\.");
				int domainLength = domains.length;
				String rootDomain = null;
				if(domainLength > 2) {
						 rootDomain = domain.substring(domain.indexOf(".")+1); 
				}else{
					     rootDomain = domain;
				}
				rootDomain = rootDomain != null ? rootDomain : "null.com";
				
				String timestamp = ilst.get(12).toString().trim();
				long hour = 10;
				try {
					hour = Long.parseLong(timestamp.substring(8, 10));
				} catch (Exception e) {
					; ;
				}
				
				if((hour >= 21 && hour <= 23) || (hour >= 0 && hour <= 7)){
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								url,true,false,map);
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 3,
								cookie,false,true,map);
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								pack_contnt,false,false,map);
				}else{
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 1,
								url,true,false,map);
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 2,
								cookie,false,true,map);
					 IdentifierGMapper.IdentifierSub(context, sUserName, notkmap,
								app, domain, rootDomain, 1,
								pack_contnt,false,false,map);
				}
			}
		}
		catch (Exception e)  {;}
	}
	
	    /** 
	     * 解码 
	     * @param str 
	     * @return string 
	     */  
	    private  byte[] decode(String str){  
	    byte[] bt = null;  
	    try {  
	        sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();  
	        bt = decoder.decodeBuffer( str );  
	    } catch (IOException e) {  
	       ;;
	    }  
	        return bt;  
	    }  
	 
	 
}
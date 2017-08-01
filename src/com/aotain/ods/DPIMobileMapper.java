package com.aotain.ods;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;
import com.aotain.ods.ua.ParseUA;

public class DPIMobileMapper extends Mapper<LongWritable,Text,Text,Text>{

	public void map(LongWritable key,Text value,Context context){

		 try {		
		 //String strKey = "";
		 //
		 //String strDomain = context.getConfiguration().get("app.domains");
		 //String strCurDate = context.getConfiguration().get("app.date");
		 //String[] arrDomain = strDomain.split("\\|");
		 String sKWConf = context.getConfiguration().get("app.keyword");
		 String strAppConf = context.getConfiguration().get("app.appflag");
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 /**
		  *0 Y001	访问互联网业务的某个会话的开始时间，格式为Yyyymmddhhmiss（24小时制）。
		   1 Y002	用户的国际移动用户识别码
		   2 Y003	用户手机号码，即MDN。
		   3 Y004	用户访问的目标网站的URL。
		   4 Y005	用户访问的目标网站的IP地址，采用点分十进制表示法。注：IPv6地址采用十六进制表示
		   5 Y006	用户访问的目标网站的端口号。
		   6 Y007	用户访问外部网站时使用的IP地址，采用点分十进制表示法。注：IPv6地址采用十六进制表示
		   7 Y008	用户访问外部网站时使用的端口号
		   8 Y009	外部网站的域名
		   9 Y010	业务标识编号（预留）
		   10 Y011	业务标识名称（预留）
		   11 Y012	协议类型
		  */
		 if(items.length != 12)
			 return;
		 
		 //验证用户账号的合法性
		 //if(!validateUser(items[1]))
		//	 return;
		 String sAccessTime = items[0];
		 String sMDN = items[2];
		 
		 //byte[] bDomainName = Base64.decodeBase64(items[6]);
		 String sDomainName = items[8];
		 String sUrl = new String(items[3]);
		 
		 String sUrlKeyword = CommonFunction.parseKeyWord(sUrl, sKWConf);
		 String appflag = CommonFunction.getAppParse(strAppConf, sDomainName);
		 
		
		 //sMDN|sDomainName|appflag|sUrlKeyword|sAccessTime
		 
		 
		 String strValue = String.format("%s|%s|%s|%s|%s", 
				 sMDN,sDomainName,appflag,sUrlKeyword,sAccessTime);
	    	
				context.write(new Text(sMDN), new Text(strValue));
			 } catch (Exception e) {
				 // TODO Auto-generated catch block
				 System.out.println(e.getMessage());
			 } 
	}
	
}

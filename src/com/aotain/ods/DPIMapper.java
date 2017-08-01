package com.aotain.ods;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;
import com.aotain.ods.ua.ParseUA;

public class DPIMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	
	
	
	
	public void map(LongWritable key,Text value,Context context){

		 try {		
		 //String strKey = "";
		 //
		 //String strDomain = context.getConfiguration().get("app.domains");
		 //String strCurDate = context.getConfiguration().get("app.date");
		 //String[] arrDomain = strDomain.split("\\|");
		 String sKWConf = context.getConfiguration().get("app.keyword");
		 //String strCookieConf = context.getConfiguration().get("app.cookie");
		 String strAppConf = context.getConfiguration().get("app.appflag");
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 /**
		  *0 UserAccount 
		   1 Protocol type
		   2 SourceIP
		   3 DestinationIP
		   4 SourcePort
		   5 DestinationPort
		   6 DomainName
		   7 URL
		   8 REFERER
		   9 UserAgent
		   10 Cookie
		   11 AccessTime
		  */
		 if(items.length != 12)
			 return;
		 
		 //验证用户账号的合法性
		 //if(!validateUser(items[1]))
		//	 return;
		 String sUserAccount = items[0];
		 
		 byte[] bDomainName = Base64.decodeBase64(items[6]);
		 String sDomainName = new String(bDomainName);
		 
		 byte[] bUrl = Base64.decodeBase64(items[7]);
		 String sUrl = new String(bUrl);
		 
		 sUrl = sUrl.replaceAll("\\|", ",");
		 
		 byte[] bRefer = Base64.decodeBase64(items[8]);
		 String sRefer = new String(bRefer);
		 
		 byte[] bUA = Base64.decodeBase64(items[9]);
		 String sUA = new String(bUA);
		 //ParseUA ua = new ParseUA();
		 sUA = "ppp";//OS|OSVersion|Device
		 
		 String OS = sUA.split("\\|",-1)[0].trim();
		 String OSVersion = sUA.split("\\|",-1)[1].trim();
		 String Device = sUA.split("\\|",-1)[2].trim();
		 String DeviceType = "";
		 if(sUA.split("\\|",-1).length>3)
			 DeviceType = sUA.split("\\|",-1)[3].trim();
		 
		 byte[] bCookie = Base64.decodeBase64(items[10]);
		 String sCookie = new String(bCookie);
		 sCookie = sCookie.replaceAll("\\|", ",");
		 //sCookie = CommonFunction.parseCookie(strCookieConf, sDomainName, sCookie);
		 
		 //sCookie = CommonFunction.parseCookie(sCookieConf, sDomainName, sCookie);
		 
		 String sAccessTime = items[11];
		 
		 String sUrlKeyword = CommonFunction.parseKeyWord(sUrl, sKWConf);
		 String sReferKeyword = CommonFunction.parseKeyWord(sRefer, sKWConf);
		 
		 
		 String appflag = CommonFunction.getAppParse(strAppConf, sDomainName);
		 
		 String referdomain = "";//CommonFunction.getDomain(sRefer);
		 String referappflag =  "";//CommonFunction.getAppParse(strAppConf, referdomain);
		 
		 //USERACCOUNT|DomainName|URL|referdomain|REFFER|OS|OSVersion|Device|Cookie|sAccessTime|
		 //url_keyword|refer_keyword|appflag|referappflag|DeviceType
		 
		 
		 String strValue = String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s", 
				 sUserAccount,sDomainName,sUrl,referdomain,sRefer,
				 OS,OSVersion,Device,sCookie,sAccessTime,
				 sUrlKeyword,sReferKeyword,appflag,referappflag,DeviceType);
	    	
				context.write(new Text(sUserAccount+OS+OSVersion+Device), new Text(strValue));
			 } catch (Exception e) {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 } 
	}
	

}

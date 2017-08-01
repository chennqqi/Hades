package com.aotain.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

public class CommonFunction {

	/**
	 * 锟斤拷取某锟斤拷锟斤拷围锟节碉拷锟斤拷锟斤拷锟�
	 * @param a
	 * @param b
	 * @return
	 */public static void main(String[] args) {
		try {
			System.out.println(java.net.URLDecoder.decode("https://192.168.9.247:8443/svn/Work/%E5%90%8E%E5%8F%B0%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F","utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(decodeBASE64("ZmZmZmZkMmYxYjQ4MDgyNjhiNWMwZGFjMjBkMDY1NzRmNjVlNDkyMywsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws5L2T6IKy5ZCN56uZLCws6KeG6aKRLCwsLOi0oue7j+ivgeWIuCwsLCwsLCwsLCwsLCwsLCwsMjAxNjExMDEsCQ=="));
	}
	public static String getQQNumber(String cookievalue){
		try {
		  Pattern pattern = Pattern.compile("([1-9]\\d{4,11})");
	      Matcher  matcher = pattern.matcher(cookievalue);
	      if(matcher.find()){
	        cookievalue=matcher.group(0);  
	    	}else{
	    	  cookievalue=null;
	      }
	      }catch(Exception e){
	    	  cookievalue=null;
	      }
	      return cookievalue;
	}
	public static int generateRandom(int a, int b) {
	        int temp = 0;
	        try {
	            if (a > b) {
	                temp = new Random().nextInt(a - b);
	                return temp + b;
	            } else {
	                temp = new Random().nextInt(b - a);
	                return temp + a;
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return temp + a;
	 }
	 
	 
	 /**
	  * 锟叫讹拷锟街凤拷锟角凤拷锟斤拷锟斤拷锟斤拷
	  *
	  * @param c 锟街凤拷
	  * @return 锟角凤拷锟斤拷锟斤拷锟斤拷
	  */
	 public static boolean isChinese(char c) {
	     Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
	     if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
	             || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
	             || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
	             || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
	             || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
	             || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
	         return true;
	     }
	     return false;
	 }
	  
	 /**
	  * 锟叫讹拷锟街凤拷锟角凤拷锟斤拷锟斤拷锟斤拷
	  *
	  * @param strName 锟街凤拷
	  * @return 锟角凤拷锟斤拷锟斤拷锟斤拷
	  */
	 public static boolean isMessyCode(String strName) {
		 
	     Pattern p = Pattern.compile("\\s*|\t*|\r*|\n*");
	     Matcher m = p.matcher(strName);
	     String after = m.replaceAll("");
	     String temp = after.replaceAll("\\p{P}", "");
	     char[] ch = temp.trim().toCharArray();
	     float chLength = ch.length;
	     float count = 0;
	     for (int i = 0; i < ch.length; i++) {
	         char c = ch[i];
	         if (!Character.isLetterOrDigit(c)) {
	             if (!isChinese(c)) {
	                 count = count + 1;
	             }
	         }
	     }
	     float result = count / chLength;
	     if (result > 0.1) {
	         return true;
	     } else {
	         return false;
	     }
	  
	 }
	  
	 /**
	  * 锟斤拷锟斤拷锟斤拷式
	  * @param str
	  * @param regEx
	  * @param group
	  * @return
	  */
	 public static String findByRegex(String str, String regEx, int group)
	 	{
	 		String resultValue = null;
	 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
	 			return resultValue;
	 		
	 		
	 		Pattern p = Pattern.compile(regEx);
	 		Matcher m = p.matcher(str);

	 		boolean result = m.matches();
	 		if (result)
	 		{
	 			resultValue = m.group(group);
	 		}
	 		return resultValue;
	 	}
	 

	/**
	 * 锟街凤拷匹锟斤拷
	 * @param strA
	 * @param strB
	 * @return
	 */
	public static float compare(String strA,String strB) {  
			 int d[][]; // 锟斤拷锟斤拷
			  int n = strA.length();
			  int m = strB.length();
			  int i; // 锟斤拷锟斤拷str锟斤拷
			  int j; // 锟斤拷锟斤拷target锟斤拷
			  
			  //锟斤拷锟斤拷锟斤拷要匹锟斤拷锟斤拷址锟斤拷
			  //int matchlength = (int)((float)Math.min(n, m)*0.7);
			  //if(Math.min(n, m)<=4)
				//  matchlength = Math.min(n, m);
			  
			  
			  char ch1; // str锟斤拷
			  char ch2; // target锟斤拷
			  
			  
				 
			  int temp; // 锟斤拷录锟斤拷同锟街凤拷,锟斤拷某锟斤拷锟斤拷锟斤拷位锟斤拷值锟斤拷锟斤拷锟斤拷,锟斤拷锟斤拷0锟斤拷锟斤拷1
			  if (n == 0) {
				  return m;
			  }
			  if (m == 0) {
				  return n;
			  }
			 
			  //n = n - matchlength + 1;
			  //m = m - matchlength + 1;
			 
			  
			  d = new int[n + 1][m + 1];
			  for (i = 0; i <= n; i++) { // 锟斤拷始锟斤拷锟斤拷一锟斤拷
				  d[i][0] = i;
			  }
			 
			  for (j = 0; j <= m; j++) { // 锟斤拷始锟斤拷锟斤拷一锟斤拷
				  d[0][j] = j;
			  }
			 
			  for (i = 1; i <= n; i++) { // 锟斤拷锟斤拷str
				  ch1 = strA.charAt(i - 1);
				  // 去匹锟斤拷target
				  for (j = 1; j <= m; j++) {
					  ch2 = strB.charAt(j - 1);
					  if (ch1==ch2) {
						  temp = 0;
					  } else {
						  temp = 1;
					  }
					  // 锟斤拷锟�+1,锟较憋拷+1, 锟斤拷锟较斤拷+temp取锟斤拷小
					  d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
				  }
			  	}
			  	return d[n][m];
			 }
			 
	private static int min(int one, int two, int three) {
		return (one = one < two ? one : two) < three ? one : three;
	}
			 
	 /**
	  * Levenshtein 锟姐法锟斤拷锟街凤拷锟斤拷锟狡讹拷
	  * @param str
	  * @param target
	  * @return
	  */
	 public static float SimilarLevenshtein(String str, String target) {				 
		 //str = str.split(" ")[str.split(" ").length - 1];
		 //target = target.split(" ")[target.split(" ").length - 1];		 
		 if(Math.max(str.length(), target.length())<= 4)
		 {
			 if(str.equals(target))
				 return 1;	 
			 return 0;
		 }
		 return 1 - (float)compare(str, target)/Math.max(str.length(), target.length());
	 }
			 
	 /**
	  * 锟筋长锟斤拷锟斤拷锟接达拷
	  * @param str1
	  * @param str2
	  * @return
	  */
	 private static int getCommonStrLength(String str1, String str2) {
		 str1 = str1.toLowerCase();  
		 str2 = str2.toLowerCase();  
		 int len1 = str1.length();  
		 int len2 = str2.length();  
		 String min = null;  
		 String max = null;  
		 String target = null;
		 min = len1 <= len2 ? str1 : str2;
		 max = len1 >  len2 ? str1 : str2;
		 //锟斤拷锟斤拷悖簃in锟接达拷锟侥筹拷锟饺ｏ拷锟斤拷锟斤拷蟪ざ瓤锟绞�
		 for (int i = min.length(); i >= 1; i--) {
			 //锟斤拷锟斤拷锟轿猧锟斤拷min锟接达拷锟斤拷锟斤拷0锟斤拷始
			 for (int j = 0; j <= min.length() - i; j++) {  
				 target = min.substring(j, j + i);  
				 //锟斤拷锟斤拷锟轿猧锟斤拷max锟接达拷锟斤拷锟叫讹拷锟角凤拷锟斤拷target锟接达拷锟斤拷同锟斤拷锟斤拷0锟斤拷始
				 for (int k = 0; k <= max.length() - i; k++) {  
					 if (max.substring(k,k + i).equals(target)) {  
						 return i;  
					 }
				 }
			 }
		 }  
		 return 0;  
	}
			 
	/**
	 * 锟筋长锟斤拷锟斤拷锟接达拷
	 * @param str1
	 * @param str2
	 * @return
	 */
	public static float SimilarCommonStrLength(String str1, String str2)
	{
		return (float)getCommonStrLength(str1,str2)/Math.max(str1.length() , str2.length()); 
	}
			 
	public static float Similar(String str1, String str2)
	{
		if(str2.length() == 0 || str1.length() ==0)
			return 0;
				 
		boolean next = false;
		if(str1.contains(str2))
		{
			next = true;
			//String str = StringUtils.rightPad(str2, str1.length() ,"0");
			//str1 = str1.replace(str2, str);
			//str2 = str;	 
		}
		else if(str2.contains(str1))
		{
			next = true;
			//String str = StringUtils.rightPad(str1, str2.length(),"0");
			//str2 = str2.replace(str1, str);
			//str1 = str;
		}
				 
		if(next)
		{//锟斤拷锟斤拷锟斤拷锟斤拷蠊锟斤拷址锟斤拷锟斤拷锟斤拷贫锟�
			float f = CommonFunction.SimilarCommonStrLength(str1,str2);
			return f;
		}
		return 0;
	}
			 
	public static String parseCookie(String cookieConfig,String domain,String cookie){
		String cookievalue="";
		if(cookieConfig.indexOf(domain)!=-1){
			String searchcookiename=cookieConfig.substring(cookieConfig.indexOf(domain));
			if(searchcookiename.indexOf("#")!=-1)
				searchcookiename=searchcookiename.substring(0,searchcookiename.indexOf("#"));
			searchcookiename=searchcookiename.split("=")[1];
			if((cookie.indexOf(searchcookiename)!=-1)){
				cookievalue=cookie.substring(cookie.indexOf(searchcookiename));
				if(cookievalue.indexOf(";")!=-1)
					cookievalue=cookievalue.substring(0,cookievalue.indexOf(";"));
				cookievalue=cookievalue.split("=")[1];
			}
		}
		return cookievalue;
	}
			 
			 
	/***
	 * Gets the URL domain name
	 * @param url      eg:http://www.jfox.info
	 * @return domain  eg:www.jfox.info
	 */
	 public static String getDomain(String url) {
		 try{
			 Pattern pattern = Pattern.compile("^((https|http|ftp|rtsp|mms|mp3|url)?://)");
			 Matcher matcher = pattern.matcher(url);
			 if (!matcher.find()) {
				 url = "http://" + url;
			 }
			 url = new URL(url).getHost().toLowerCase();
		 }catch(Exception ex){
			 return "";
		 }
		 return url;
	 }
				
	 /**
	  * Base64 锟斤拷锟斤拷
	  * @param s
	  * @return
	  */
	 public static String decodeBASE64(String s) { 
		 if (s == null)
			 return null; 
		 byte[] b = Base64.decodeBase64(s);
		 String sReturn = new String(b);
		 return sReturn;
	 }
				
	 /**
		 * 锟斤拷锟斤拷转锟斤拷为IP
		 * @param ipInt
		 * @return
		 */
		public static String int2ip(long ipInt){
			
			StringBuilder sb=new StringBuilder();
			sb.append((ipInt>>24)&0xFF).append(".");
			sb.append((ipInt>>16)&0xFF).append(".");
			sb.append((ipInt>>8)&0xFF).append(".");
			sb.append(ipInt&0xFF);
			return sb.toString(); 
			
		} 
		
				   
	/**
	 * 应锟矫憋拷签
	 * @param app
	 * @param domain
	 * @return
	 * @throws Exception
	 */
	public static String getAppParse(String app, String domain) throws Exception
	{
		String appclassid="",tempdom,temp;
		int pos = -1;
		String[] plist = app.split(";");
		for(int index=0;index<plist.length;index++)
		{
			temp = plist[index];
			tempdom = temp.substring(0,temp.indexOf("="));
			pos = domain.indexOf(tempdom);
			if(pos>-1)
			{
				appclassid = temp.substring(tempdom.length()+1,temp.length());
				break;
			}
		}
		if(appclassid.length()<1)
			appclassid = "12_0";
		return appclassid;
	}
			
	/**
	 * MD5 锟斤拷锟斤拷
	 * @param plainText
	 * @return
	 */
	 public static String md5s(String plainText) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte b[] = md.digest();

            int i;

            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
            	i = b[offset];
            	if (i < 0)
            		i += 256;
            	if (i < 16)
            		buf.append("0");
            	buf.append(Integer.toHexString(i));
            }
            String str = buf.toString();
            	return str;
					   //System.out.println("result: " + buf.toString());// 32位锟侥硷拷锟斤拷
					   //System.out.println("result: " + buf.toString().substring(8, 24));// 16位锟侥硷拷锟斤拷
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
			}
	 }
			 
	/**
	 * 
	 * @param urlconfig
	 * @return
	 */
	 public static HashMap<String,String> getUrlClassConfig(String urlconfig)
	 {
			String[] arrClass = urlconfig.split(";",-1);
			HashMap<String,String> hmClass = new HashMap<String, String>();
			for(String s : arrClass)
			{
				if(s.split("=",-1).length < 2)
					continue;
				hmClass.put(s.split("=",-1)[0],s.split("=",-1)[1]);
			}			
			return hmClass;
	 }
		
	 /**
	  * 锟斤拷取URL锟斤拷签
	  * @param hmConfig
	  * @param domain
	  * @return
	  */
	 public static String getUrlClass(HashMap<String,String> hmConfig,String domain)
	 {
			if(hmConfig.containsKey(domain))
			{
				return hmConfig.get(domain);
			}
			else
			{
				return "";
			}
	 }
			 
				
	//应锟斤拷map锟斤拷锟斤拷 
	public static String appparse(String app, String domain) throws Exception
	{
		String appclassid="",tempdom,temp;
		int pos = -1;
		String[] plist = app.split(";");
		for(int index=0;index<plist.length;index++)
		{
			temp = plist[index];
			tempdom = temp.substring(0,temp.indexOf("="));
			pos = domain.indexOf(tempdom);
			if(pos>-1)
			{
				appclassid = temp.substring(tempdom.length()+1,temp.length());
				break;
			}
		}
		if(appclassid.length()<1)
			appclassid = "12_0";
		return appclassid;
	}
	
	/**
	 * 锟斤拷锟斤拷锟截硷拷锟斤拷
	 * @param url
	 * @param cf
	 * @return
	 */
	public static String parseKeyWord(String url,String cf,String domain){
		try{
		String kw="";
		String[] cols =cf.split("\\#");
		int exist = -1;
		int pos =0;
		for(String temp:cols){
			String[] rds = temp.split("=");
			if(domain.contains(rds[0])){
				exist=0;
				break;
			}
			pos++;
		}
		if(exist == 0 ){
			if(pos<cols.length){
				String[] rd = cols[pos].split("\\=");
						if(rd.length>=2){
						boolean multiple =rd[1].contains(",");
						if(multiple){
							String[] mulp = rd[1].split(",");
							for(String temp:mulp){
								 if (url.indexOf("?"+temp+"=") > 0 || url.indexOf("&"+temp+"=") > 0)
					             {	
									 int tp =url.indexOf("?"+temp+"=") > 0 ? url.indexOf("?"+temp+"=") : url.indexOf("&"+temp+"=");
									 String  tempkw =url.substring(tp+temp.length()+2);
									 if(tempkw.contains("&")){
										 tempkw = tempkw.substring(0, tempkw.indexOf("&"));
									 }else{
										 tempkw=tempkw.substring(0);
									 }
									 byte[] buf = CommonFunction.GetUrlCodingToBytes(tempkw);
									 try {
										 if(CommonFunction.IsUTF8(buf)){
											   int myindex=kw.lastIndexOf("%");
												if(myindex!=-1){
													kw=kw.substring(0,myindex);
												}
												kw = new String( java.net.URLDecoder.decode(tempkw,"utf-8"));
												
										}else{
											int myindex=kw.lastIndexOf("%");
											if(myindex!=-1){
												kw=kw.substring(0,myindex);
											}
											kw = new String( java.net.URLDecoder.decode(tempkw,"gbk"));
											
										}
									} catch (Exception e) {
										// TODO Auto-generated catch block
										//e.printStackTrace();
										//System.out.println(tempkw);
									}
									 break;
					             }
							}
						}
						else{
							 if (url.indexOf("?"+rd[1]+"=") > 0 || url.indexOf("&"+rd[1]+"=") > 0)
				             {
								 int tp =url.indexOf("?"+rd[1]+"=") > 0 ? url.indexOf("?"+rd[1]+"=") : url.indexOf("&"+rd[1]+"=");
								 String  tempkw =url.substring(tp + rd[1].length()+2);
								 if(tempkw.contains("&")){
									 tempkw = tempkw.substring(0, tempkw.indexOf("&"));
								 }else{
									 tempkw=tempkw.substring(0);
								 }
								byte[] buf = CommonFunction.GetUrlCodingToBytes(tempkw);
								 try {
									 if(CommonFunction.IsUTF8(buf)){
										 int myindex=kw.lastIndexOf("%");
											if(myindex!=-1){
												kw=kw.substring(0,myindex);
											}
											kw = new String( java.net.URLDecoder.decode(tempkw,"utf-8"));
									}else{
										int myindex=kw.lastIndexOf("%");
										if(myindex!=-1){
											kw=kw.substring(0,myindex);
										}
										kw = new String( java.net.URLDecoder.decode(tempkw,"gbk"));
									}
								} catch (Exception e) {
									// TODO Auto-generated catch block
									//System.out.println(tempkw);
									//se.printStackTrace();
								}
				             }
						}
					}
				}
		}
		 int keywordlength=kw.length();
		 if(keywordlength<59&&keywordlength>1)
	        return kw;
		 else
			return "";
		 }catch(Exception e){
			// e.printStackTrace();
		 }
		return "";
		
	   }


	/**
	 * 锟叫讹拷锟角凤拷UTF8
	 * @param buf
	 * @return
	 */
	public static boolean IsUTF8(byte[] buf) { 

		int score = 0;

		int i, rawtextlen = 0;

		int goodbytes = 0, asciibytes = 0;

		rawtextlen = buf.length;

		for (i = 0; i < rawtextlen; i++) {
			if ((buf[i] & (byte) 0x7F) == buf[i]) {
				// 锟斤拷锟轿伙拷锟�0锟斤拷ASCII锟街凤拷
				asciibytes++;
			} else if (-64 <= buf[i] && buf[i] <= -33
					&& i + 1 < rawtextlen && -128 <= buf[i + 1]
					&& buf[i + 1] <= -65) {
				goodbytes += 2;
				i++;

			} else if (-32 <= buf[i]&& buf[i] <= -17
			&&i + 2 < rawtextlen && -128 <= buf[i + 1]
			&& buf[i + 1] <= -65 && -128 <= buf[i + 2]
			&& buf[i + 2] <= -65) {
				goodbytes += 3;
				i += 2;
			}
		}
		if (asciibytes == rawtextlen) {
			return false;
		}
		score = 100 * goodbytes / (rawtextlen - asciibytes);
		if (score > 98) {
			return true;
		} else if (score > 95 && goodbytes > 30) {
			return true;
		} else {
			return false;
		}
	}
				
	/**
	 * url 锟斤拷锟斤拷转锟斤拷 bytes
	 * @param url
	 * @return
	 */
	public static byte[] GetUrlCodingToBytes(String url)
	{
		StringBuilder sb = new StringBuilder();
		int i = url.indexOf("%");
		while (i >= 0)
		{
			if (url.length() < i + 3)
			{
				break;
			}
			sb.append(url.substring(i, i + 3));
			url = url.substring(i + 3);
			i = url.indexOf("%");
		}
		
		String urlCoding = sb.toString();
		if (urlCoding.isEmpty())
			return new byte[0];
		urlCoding = urlCoding.replace("%", "");

		int len = urlCoding.length() / 2;
		byte[] result = new byte[len];
		len *= 2;
		for (int index = 0; index < len; index++)
		{
			String s = urlCoding.substring(index, index + 2);
			int b = Integer.parseInt(s, 16);  
			
			//int b = int.Parse(s, System.Globalization.NumberStyles.HexNumber);
			result[index / 2] = (byte)b;
			index++;
		}
		return result;
	}	
	
	
	public static void getUrlFliterPostfix(String postfix,Configuration conf)
	{
		try
		{
			Map<String,String> map=new HashMap<String,String>();
			String[] fixs = postfix.split(",",-1);
			for(String fix : fixs)
			{
				map.put(fix, fix);
			}
			
			conf.set("app.postfix",
	                ObjectSerializer.serialize((Serializable) map));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public static void getUrlFlag(String confUriURLClass,
			Configuration conf){
		File file = new File(confUriURLClass);
		
		InputStreamReader in = null;
		//StringBuffer pzFile = new StringBuffer();
		try{
			Map<String,String> map=new HashMap<String,String>();
			if(file.isFile() && file.exists()){//锟叫讹拷锟角凤拷锟斤拷锟侥硷拷
			//锟斤拷锟解汉锟街憋拷锟斤拷锟斤拷锟斤拷
			in = new InputStreamReader(new FileInputStream(file),"UTF8");
			BufferedReader buffer = new BufferedReader(in);
			String lineText = "";
			while((lineText = buffer.readLine()) != null){
				String item[] = lineText.split("\\|",-1);
				if(item.length<5)
					continue;
				map.put(item[4], item[2]);
				//System.out.println(item[4] + "|" + item[2]);
				}
			}
			
			conf.set("app.domains",
	                ObjectSerializer.serialize((Serializable) map));
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void getUrlFlag(String confUriURLClass,
			String dbdriver,
			String dburl,
			String dbuser,
			String dbpassword,
			Configuration conf)
	{
		ResultSet rs= null;
		Connection con = null;
		PreparedStatement ps = null;
		try
		{
			Map<String,String> map=new HashMap<String,String>();
			String fieldsplit = dbdriver + "," + dburl + "," + dbuser + "," + dbpassword;
			Class.forName(fieldsplit.split(",")[0]);
			con = DriverManager.getConnection(fieldsplit.split(",")[1],fieldsplit.split(",")[2],fieldsplit.split(",")[3]);
			ps = con.prepareStatement("select class_id,host from to_url_class");
			rs= ps.executeQuery();
	        while(rs.next()){
	        	map.put(rs.getString("host"), rs.getString("class_id"));
	        }
	        
			//conf.set("app.domains", sburl.toString());
			
			conf.set("app.domains",
	                ObjectSerializer.serialize((Serializable) map));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			try {
				if(rs!=null)
					rs.close();
				if(ps!=null)
					ps.close();
				if(con!=null)
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	
	/** 
	 * IP转锟斤拷锟斤拷锟斤拷 
	 * @param ip 
	 * @return 
	 */  
	public static Long ip2int(String ip)   
	{  
	    Long num = 0L;  
	    if (ip == null){  
	        return num;  
	    }  
	      
	    try{  
	        ip = ip.replaceAll("[^0-9\\.]", ""); //去锟斤拷锟街凤拷前锟侥匡拷锟街凤拷  
	        String[] ips = ip.split("\\.");  
	        if (ips.length == 4){  
	            num = Long.parseLong(ips[0], 10) * 256L * 256L * 256L + Long.parseLong(ips[1], 10) * 256L * 256L + Long.parseLong(ips[2], 10) * 256L + Long.parseLong(ips[3], 10);  
	            num = num >>> 0;  
	        }  
	    }catch(NullPointerException ex){  
	        System.out.println(ip);  
	    }  
	      
	    return num;  
	}  
	
	/**
	 * 字符串是否为空
	 * @param str
	 * @return
	 */
	public static boolean isNull(String str) {
		return null == str || str.trim().equals("");
	}
	
	/**
	 * 是否包含中文
	 * @param str
	 * @return
	 */
	public static boolean isContainChinese(String str) {
		return Pattern.compile("[\u4e00-\u9fa5]").matcher(str).find();
	}
}

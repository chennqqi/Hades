package com.aotain.project.ud3clear;


import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class ParseUA {
	
	public  ArrayList<String> osRegexs = new ArrayList<String>();
	
	public  ArrayList<String> osVersionRegexs = new ArrayList<String>();
	
	public   ArrayList<String> deviceRegexs = new ArrayList<String>();
	
	public   ArrayList<String> browserRegexs = new ArrayList<String>();
	
	public   ArrayList<String> browserVersionRegexs = new ArrayList<String>();
	
	public ParseUA(ArrayList<String> osRegexs,ArrayList<String> osVersionRegexs ,
			 ArrayList<String> deviceRegexs,ArrayList<String> browserRegexs , ArrayList<String> browserVersionRegexs){
		this.osRegexs=osRegexs;
		this.osVersionRegexs=osVersionRegexs;
		this.deviceRegexs=deviceRegexs;
		this.browserRegexs=browserRegexs;
		this.browserVersionRegexs=browserVersionRegexs;
	}
	
	public static void main(String[] args) {
//		String ua="ting_v4.3.20_c5(CFNetwork, iPhone OS 8.3, iPhone5,2)";		
//		System.out.println(getUAinfo(ua));		
	}

	public  String getUAinfo(String ua)  {
		 
//		ua="nilvik/v3.3.63_update5 (Linux; U; Android 3.0.2-RS-20151026.1730; MagicBox1s_Plus Build/KOT49H)";
//		ua="Mozilla/5.0 (Linux; U; Android 4.1.2; zh-cn; SCH-I739 Build/JZO54K) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30";
//		ua="ting_v1.0.5_id763(CFNetwork, iPhone OS 8.2, iPhone7,2)";
//		ua="Mozilla/5.0 (iPad; U; iQiyiAd; zh-CN) iQiyiAd/4.5.011";
//		ua="Mozilla/5.0 (iPhone; CPU iPhone OS 8_0_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12A405 WangZhiDaQuanIphone 1.3.2";
//		ua="iPhone6,2/8.0 (12A365)";
//		ua="compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQDownload 734; .NET CLR 2.0.50727; InfoPath.2";
//		ua="Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Mobile/11D201 rabbit%2F1.0 baiduboxapp/0_0.1.9.6_enohpi_069_046/1.1.7_1C2%254enohPi/1099a/5794";
//		ua="HD 1.5.0 rv:27000 (iPad; iPhone OS 9.1; zh_CN)";
//		ua="ting_v4.3.20_c5(CFNetwork, iPhone OS 8.3, iPhone5,2)";		
//		ua="66589871aa7301610e1jpgT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36";
//		ua="Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.9.0.7) Gecko/2009031915 Gentoo Firefox/3.0.7";
//		ua="Mozilla/4.0 (compatible; MSIE 6.0; MS Web Services Client Protocol 2.0.50727.5483)";
//		ua="Mozilla/5.0 (Linux; U; Linux i686; zh-cn; EC6108V1 Build/FRF91) AppleWebKit/2.0 (KHTML, like Gecko) EC1308 hybroad; Resolution(PAL)";
//		ua="Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:33.0) Gecko/20100101 Firefox/33.0";
//		ua="GT-I9505; 4.4.2; KOT49H.I9505ZHUFNB3; zh-cn";
		
//		System.out.println("ua>>>>"+ua);
		String os = null;
		String osV = null;
		String device = null;
		String browser = null;
		String browserV = null;
		
/*		osRegexs.add("(Windows|Android|iPhone|iPad|iOS|Macintosh|Linux){1}");		//��Щֵ��ʱ��������ļ���?
		
		osVersionRegexs.add("Windows\\s+(NT\\s+[0-9]{0,2}\\.[0-9]+)");
		osVersionRegexs.add("Android[;|/|\\s*](\\d.?\\d?.?\\d?)");	
		osVersionRegexs.add("iOS([0-9]\\.[0-9]\\.[0-9])");
		osVersionRegexs.add("OS\\s+(\\d{1}[\\.|_]?\\d?[\\.|_]?\\d?)");
		osVersionRegexs.add("(iphone)\\s?;\\s?(\\d+\\.?\\d*\\.?\\d*)");
		osVersionRegexs.add("(?<=iPhone\\d?)\\s*[,|;]\\s*\\d+/(\\d+\\.?\\d?\\.?\\d?)");
		osVersionRegexs.add("(?<=OS|ipad)\\s*[,|;|/]\\s?(\\d{1}\\.?\\d?\\.?\\d?)");
		osVersionRegexs.add("(?<=Linux)\\s*(\\w+)");
		
		deviceRegexs.add("(?<=netdisk)[,|;]{1}\\d+\\.?\\d*\\.?\\d*[,|;]{1}(\\w+(\\-|\\s)*\\w*);");
//		deviceRegexs.add("([^0-9]*[\\-]*[a-zA-Z0-9]*+\\s+)(Build){1}");
		deviceRegexs.add(";\\s*(\\w+\\-*\\w*)\\s*(?=Build)");
		deviceRegexs.add("((?<!\\w)(iPad|iPhone)\\s*\\d{1}[s]?)");
		deviceRegexs.add("(iPad|iPhone)\\s*(?!\\d)");
		deviceRegexs.add("(\\w*\\-?\\w*);\\s*\\d{1}\\.?\\d?\\.?\\d?.+(?=zh-cn)");
								
		browserRegexs.add("(Opera|Chrome|Safari|Firefox|NetFront|Lynx|Explore|MQQBrowser|UCBrowser|BIDUBrowser|MSIE)");
		
		browserVersionRegexs.add("(?<=Opera|Chrome|Safari|Firefox|NetFront|Lynx|MQQBrowser|UCBrowser|BIDUBrowser|MSIE)[/|\\s*]([0-9]+\\.?[0-9]*\\.?[0-9]*\\.?[0-9]*)");
		*/		
		for(String regex :osRegexs){
			os = parseUA(ua,regex);
			
			if(os !=null){
			
				break;		
			}
		}
		
	for(String regex :osVersionRegexs){
			osV = parseUA(ua,regex);
			
			if(osV !=null){
				
				break;
			}
		}
//		
		for(String regex :deviceRegexs){
			device = parseUA(ua,regex);
			
			if(device !=null){
				
				break;
			}
		}
		
		for(String regex :browserRegexs){
			browser = parseUA(ua,regex);
		
			if(browser !=null){
				
				break;
			}
		}
		
		for(String regex :browserVersionRegexs){
			browserV = parseUA(ua,regex);
			
	if(browserV !=null){
//			
		break;
			}
		}  
		
		
		return os +","+ osV+","+device+","+browser+","+browserV;
		
	}

	public  String parseUA(String ua ,String regex ){
		
		try{
//			System.out.println("99999999999999");
			 Pattern p = Pattern.compile(regex,Pattern.CASE_INSENSITIVE);
		        Matcher m = p.matcher(ua);        
		       
		     if(m.find()) {
//		        	 System.out.println("m.groupCount()>>>"+m.groupCount());
//		        	System.out.println("m.group()>>>"+m.group());
		        	return m.group(1);
		        }  
//		        return null;    
		}catch(Exception e){
			e.printStackTrace();
		}         
        return null;
	}	

}

package com.aotain.project.other;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class TmpHttpMapper extends Mapper<LongWritable, Text, Text, Text>{

	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		String[] items = value.toString().split("\\|", -1);
		try{
			String custIps = "IPCYW2079930880|113.98.60.8-13,IPCYW2077456050|183.62.234.32-93,IPCYW2054043218|61.144.211.32-61," +
					"IPCYW2126437598|121.15.168.48-55,IPCYW2060236278|83.62.179.160-165";
			String[] custIpsArray = custIps.split(",",-1);
			 /**
			  *0 AreaID 
			   1 UserName
			   2 SrcIP
			   3 Domain
			   4 Url
			   5 Refer
			   6 OperSys
			   7 OperSysVer
			   8 Browser
			   9 BrowserVer
			   10 Device
			   11 AccessTime
			   12 Cookie
			   13 Keyword
			   14 UrlClassID
			   15 referdomain
			   16 referclassid
			  */
			boolean flag = false;
			String tmpcustIps = "";
			 if(items.length != 17 && items.length != 15)
				 return;
			 //验证用户账号的合法性
			 if(!validateUser(items[1]))
				 return;
			 
			 String accesstime = CommonFunction.findByRegex(items[11], "[0-9]*", 0);
			 if(accesstime==null)
				 return;
			if(StringUtils.isNotEmpty(items[2])){ //
				for (String custIp : custIpsArray) {
					flag = ipIsValid(custIp.split("\\|",-1)[1],items[2]);
					if(flag){
						tmpcustIps = items[0]+"|"+custIp.split("\\|",-1)[0]+value.toString().substring(items[0].length());
						context.write(new Text(tmpcustIps), new Text(""));
						break;
					}
				}
			}
		}catch(Exception e){
			
		}
	};
	
	public  boolean ipIsValid(String ipselect, String ip) {
		try{
			ipselect = ipselect.trim();//113.98.60.8-13
			ip = ip.trim();
			String ipselectheader = ipselect.substring(0,ipselect.lastIndexOf("."));
			String ipselectend = ipselect.substring(ipselect.lastIndexOf(".")+1,ipselect.length());
			String ipheader = ip.substring(0,ip.lastIndexOf("."));
			String ipend = ip.substring(ip.lastIndexOf(".")+1,ip.length());
			String[] ipval = ipselectend.split("-");
			if(ipselectheader.equals(ipheader)){
				if((Integer.parseInt(ipend) >= Integer.parseInt(ipval[0])) && (Integer.parseInt(ipend) <= Integer.parseInt(ipval[1]))){
					return true;
				}
			}
		}catch (Exception e) {
			e.getStackTrace();
		}
		return false;
	}
	
	private boolean validateUser(String username)
	{
		boolean ret = false;
		
		String aa = CommonFunction.findByRegex(username, "^([a-zA-Z0-9_\\-\\.]{1,15})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org|gd)(\\]?)$", 0);
		if(aa == null)
		{
			aa = CommonFunction.findByRegex(username, "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}", 0);
			if(aa == null)
				ret = false;
			else
				ret = true;
		}
		else
		{
			ret = true;
		}
		
		return ret;
	}
	
}

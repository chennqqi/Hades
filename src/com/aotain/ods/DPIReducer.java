package com.aotain.ods;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class DPIReducer extends Reducer<Text,Text,Text,Text>{
	
	private static HashMap<String,String> hmClass=null;
	private static HashMap<String,String> hmPostfix=null;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("app.domains");
		  hmClass  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
		  
		  String strPostfix = context.getConfiguration().get("app.postfix");
		  hmPostfix  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(strPostfix);
	}
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		
			//String strDomain = context.getConfiguration().get("app.domains");
			//HashMap<String,String> hmClass = CommonFunction.getUrlClassConfig(strDomain);
			
			try
			{
			/**
			 * 	GUID|账号|终端型号|操作系统|访问域名|
			 * 	访问APP标签|访问URL标签|访问关键字标签|COOKIE|
				关键字|访问时间|是否噪音
				OUTPUT:
				   GUID                           STRING,
   				   AD                             STRING,
   				   DEVICE                         STRING,
   				   OSVER                          STRING,
                   CDOMAIN                        STRING,
                   URL                            STRING,
   	               CAPPTAG                        STRING,
                   CURLTAG                        STRING,
                   CKEYTAG                        STRING,
                   KEYWORD                        STRING,
                   ACCTIME                        STRING,
                   NOICE_FLAG                     int,
			 */
				
			for(Text value : values)
			{
				StringBuilder sbdResult = new StringBuilder();
				//USERACCOUNT|DomainName|URL|referdomain|REFFER|
				//OS|OSVersion|Device|Cookie|sAccessTime|
				//url_keyword|refer_keyword|appflag|referappflag|DeviceType
				
				String[] items = value.toString().split("\\|",-1);
				String useraccount = items[0];
				
				
				
				
				String domain = items[1];
				String url = items[2];
				String postfix = url.substring(url.lastIndexOf(".") + 1);
				
				
				String referdomain = items[3];
				String refer = items[4];
				String OS_V = items[5].trim() + " " + items[6].trim();
				String device = items[7].trim();
				String guid = CommonFunction.md5s(useraccount + device +OS_V.trim());
				
				sbdResult.append(guid + "|");//#0#
				sbdResult.append(useraccount + "|");//#1#
				
				sbdResult.append(device + "|");//#2#
				sbdResult.append(OS_V + "|");//#3#
				sbdResult.append(domain + "|");//#4#
				sbdResult.append(url + "|");//#5#
				
				String cookie = items[8];
				String accesstime = items[9];
				String url_keyword = items[10];
				String refer_keyword = items[11];
				String appflag = items[12];
				String urlflag = CommonFunction.getUrlClass(hmClass, domain);
				
				//refer
				
				//String referflag =  CommonFunction.getUrlClass(hmClass, referdomain);
				//String referappflag =  items[13];
				String TEMTAG =  items[14];
				
				sbdResult.append(appflag + "|");//#6#
				sbdResult.append(urlflag + "|");//#7#
				//sbdResult.append(TEMTAG + "|");
				
				String ckeytag = "";
				if(!refer_keyword.trim().isEmpty())
					ckeytag = urlflag;
				sbdResult.append(ckeytag + "|");//#8#
				//sbdResult.append(cookie + "|");//##
				sbdResult.append(refer_keyword + "|");//#9#
				sbdResult.append(accesstime + "|");//#10#
				//sbdResult.append(strCitycode + "|");
				
				if(accesstime.trim().isEmpty())
					 continue;
				
				//SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
				//Date dStartTime = new Date(Long.parseLong(accesstime)*1000L);
				//String strDate = df.format(dStartTime);
				//sbdResult.append(strDate + "|");
				
				//sbdResult.append(referdomain + "|");
				//sbdResult.append(referflag + "|");
				//sbdResult.append(referappflag + "|");
				
				//sbdResult.append("" + "|");//EKEYTAG
				String noice = "0";
				for(String fix : hmPostfix.keySet())
				{
					if(url.contains(fix))
					{
						noice = "1";
						break;
					}
				}
				
				sbdResult.append(noice + "|");//#11#是否噪音
				
				
				sbdResult.append(refer + "|");
				sbdResult.append(cookie + "|");
				
				//sbdResult.append(value.toString());
				context.write(new Text(sbdResult.toString()), new Text(""));
				
			}
			
			//context.write(new Text(strDomain), new Text(""));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
}



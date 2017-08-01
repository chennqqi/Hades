package com.aotain.project.cnshuidi;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PingAnMapper extends Mapper<LongWritable,Text,Text,Text>{
	 public void map(LongWritable key,Text value,Context context){

		 String sUrlList = context.getConfiguration().get("app.urllist");
		 //System.out.println("List:"+sUrlList);
		 //获取到需要过滤的url
		 String[] arrUrlList = sUrlList.split("\\|",-1);
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 /*
		  * AreaID
			用户账号 
			分类ID 
			SRCIP
			DESIP
			URL Key 
			二级域名 
			完整URL 
			StartTime 
			EndTime
			记录数 
			URLFROM
		  * */
		 /**新需求修改后字段结构 2014-12-24 turk
		  * AreaID
			UserName
			SrcIP
			Domain
			Url
			Refer
			OperSys
			OperSysVer
			Browser
			BrowserVer
			Device
			AccessTime
			Cookie
			Keyword
			UrlClassID
		  */
		 
		 if(items.length < 15)
			 return;
		 String URL = items[4];//完整URL
		 //System.out.println("List:"+arrUrlList.length);
		 
		 for(String url:arrUrlList)
		 {
			 
			 //System.out.println("URL:"+url);
			 if(URL.contains(url))
			 {
				 
				 
				 String userID = items[1];
				 if(!userID.contains("@"))
					 return;
			     String SRCIP = items[2];
				 //SRCIP = int2ip(Long.parseLong(SRCIP));
				 
				 String StartTime = items[11];
				 //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				 //Date dStartTime = new Date(Long.parseLong(StartTime)*1000L);
				 //String strStartTime = df.format(dStartTime);
				 //Random random = new Random(System.currentTimeMillis());
				 //int rID = Math.abs(random.nextInt(1000));//随机文件ID
				 //URL = url;
				 //宽带账户|IP|时间戳|URL
				 //String rowkey = rID+""+keyDateTime+"_"+userID+"_"+URL;
				 String rowkey = String.format("%s|%s|%s|%s", userID,SRCIP,StartTime,URL);
				 String strValue = "";
				 try {
					
					context.write(new Text(rowkey), new Text(strValue));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 break;
			 }
		 }
		 
		 
			
		 
	 }	
	 
	 
	 /**
		 * 整形转换为IP
		 * @param ipInt
		 * @return
		 */
		private static String int2ip(long ipInt){
			
			StringBuilder sb=new StringBuilder();
			sb.append((ipInt>>24)&0xFF).append(".");
			sb.append((ipInt>>16)&0xFF).append(".");
			sb.append((ipInt>>8)&0xFF).append(".");
			sb.append(ipInt&0xFF);
			return sb.toString(); 
			
		} 
}
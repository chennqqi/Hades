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
		 //��ȡ����Ҫ���˵�url
		 String[] arrUrlList = sUrlList.split("\\|",-1);
		 
		 //���ֶ�ֵ
		 String[] items = value.toString().split("\\|",-1);
		 /*
		  * AreaID
			�û��˺� 
			����ID 
			SRCIP
			DESIP
			URL Key 
			�������� 
			����URL 
			StartTime 
			EndTime
			��¼�� 
			URLFROM
		  * */
		 /**�������޸ĺ��ֶνṹ 2014-12-24 turk
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
		 String URL = items[4];//����URL
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
				 //int rID = Math.abs(random.nextInt(1000));//����ļ�ID
				 //URL = url;
				 //����˻�|IP|ʱ���|URL
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
		 * ����ת��ΪIP
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
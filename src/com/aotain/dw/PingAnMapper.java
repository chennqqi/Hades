package com.aotain.dw;

import java.io.IOException;
import java.text.ParseException;
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
		 String URL = items[7];
		 String keyUrl = items[6];
		 //System.out.println("List:"+arrUrlList.length);
		 //for(String url:arrUrlList)
		 {
			 //System.out.println("URL:"+url);
			 if(sUrlList.contains(keyUrl))
			 {
				 
				 
				 String userID = items[1];
				 String SRCIP = items[3];
				 
				 String StartTime = items[8];
				 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
				 Date dStartTime = new Date(Long.parseLong(StartTime)*1000L);
				 String keyDateTime = df.format(dStartTime);
				 Random random = new Random(System.currentTimeMillis());
				 int rID = Math.abs(random.nextInt(1000));//随机文件ID
				 URL = keyUrl;
				 
				 String rowkey = rID+""+keyDateTime+"_"+userID+"_"+URL;
				 
				 try {
					
					context.write(new Text(rowkey), new Text("UserID|"+userID));
					context.write(new Text(rowkey), new Text("SRCIP|"+SRCIP));
					context.write(new Text(rowkey), new Text("StartTime|"+StartTime));
					context.write(new Text(rowkey), new Text("URL|"+URL));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
			 //break;
		 }
		 
		 
			
		 
	 }	
}

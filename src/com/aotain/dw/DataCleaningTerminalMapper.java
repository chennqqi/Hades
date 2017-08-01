package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;

public class DataCleaningTerminalMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key,Text value,Context context){

		 
		 
		 String strKey = "";
		 //获取到需要过滤的url
		
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 /**新需求修改后字段结构 2014-12-24 turk
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
		 if(items.length != 17)
			 return;
		 
		 if(CommonFunction.isMessyCode(items[6]))
			 return;
		 
		 if(CommonFunction.isMessyCode(items[7]))
			 return;
		 
		 if(CommonFunction.isMessyCode(items[8]))
			 return;
		 
		 if(CommonFunction.isMessyCode(items[9]))
			 return;
		 
		 if(CommonFunction.isMessyCode(items[10]))
			 return;
		 
		//设备类型、操作系统同时为空，返回
		 if(items[10].trim().isEmpty()&&items[6].trim().isEmpty())
			 return;
		 
		 //UserName|OperSys|OperSysVer|Browser|BrowserVer|Device
		 strKey = String.format("%s|%s|%s|%s|%s|%s|",items[1],items[6],items[7],items[8],
				 items[9],items[10]);
		// CommonFunction.findByRegex(kv.get("CALL_TYPE"), "[0-9]*", 0)

		
	     try {			
				context.write(new Text(strKey), new Text(items[10]));
			} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
	}
}

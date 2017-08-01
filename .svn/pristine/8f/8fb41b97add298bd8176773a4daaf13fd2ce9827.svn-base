package com.aotain.ods;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.aotain.common.ObjectSerializer;

public class DPICombiner  extends Reducer<Text,Text,Text,Text>{
	
	private static HashMap<String,String> hmClass=null;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("app.postfix");
		  hmClass  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
	}
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		List<Text> save = new ArrayList<Text>();
		
		HashMap<String,String> referUrlList = new HashMap<String,String>();
		for(Text txt : values)
		{
			//行字段值
			String[] items = txt.toString().split("\\|",-1);
			//USERACCOUNT|DomainName|URL|referdomain|REFFER|
			//OS|OSVersion|Device|Cookie|sAccessTime|
			//url_keyword|refer_keyword|appflag|referappflag|DeviceType|
			//
			
			String referurl = items[4];
			if(!referurl.isEmpty() 
					&& referurl.substring(referurl.length()-1).equals("/"))
				referurl = referurl.substring(0,referurl.length() - 1);
			if(!referurl.isEmpty() && !referUrlList.containsKey(referurl))
			{
				referUrlList.put(referurl,txt.toString());
			}
			save.add(new Text(txt));
			
		}
		
		List<String> findRefer = new ArrayList<String>();
		
		for(Text s : save)
		{
			String[] items = s.toString().split("\\|",-1);
			String url = items[2];
			String postfix = url.substring(url.lastIndexOf(".") + 1);
			String cookie = items[8];
			if(cookie.trim().isEmpty())
			{//cookie为空，为噪音
				context.write(new Text(key), new Text(s + "|" + "0"));//噪声URL
				continue;
			}
			
			if(hmClass.containsKey(postfix))
			{//过滤噪音
				context.write(new Text(key), new Text(s + "|" + "0"));//噪声URL
				continue;
			}
			
			
			if(items[4].isEmpty() || referUrlList.containsKey(items[2]))
			{
				context.write(new Text(key), new Text(s + "|" + "1"));//正常URL
				if(!findRefer.contains(items[2]))
				{
					findRefer.add(items[2]);//记录已找到的refer
				}
			}
			else
			{
				context.write(new Text(key), new Text(s + "|" + "0"));//噪声URL
			}
		}
		
		/*
		for(String refer : findRefer)
		{
			if(referUrlList.containsKey(refer))
			{
				referUrlList.remove(refer);
			}
		}
		
		for(String line : referUrlList.values())
		{
			//将没有找到refer的记录 模拟一条url出来
			String[] items = line.split("\\|",-1);
			items[4] = items[5];
			//items[3] = getDomain(items[4]); // domain; 
			items[5] = "";
			items[12] = "";
			items[13] = "";
			items[14] = "0";
			if(items.length==17)
			{
				items[14] = items[16];
				items[15] = "";
				items[16] = "";
			}
			StringBuilder sb = new StringBuilder();
			for(String v : items)
			{
				sb.append(v + "|");
			}
			context.write(new Text(key),new Text(sb.toString() + "|" + "1"));
		}*/
		
	}
}

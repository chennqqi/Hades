package com.aotain.ods;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class HTTPFilterCombiner extends Reducer<Text,Text,Text,Text>{
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		List<Text> save = new ArrayList<Text>();
		
		HashMap<String,String> referUrlList = new HashMap<String,String>();
		for(Text txt : values)
		{
			//行字段值
			String[] items = txt.toString().split("\\|",-1);
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
			
			String referurl = items[5];
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
			if(items[5].isEmpty() || referUrlList.containsKey(items[4]))
			{
				context.write(new Text(s), new Text("1"));
				if(!findRefer.contains(items[4]))
				{
					findRefer.add(items[4]);//记录已找到的refer
				}
			}
			else
			{
				//context.write(new Text(s), new Text("0"));
			}
		}
		
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
			items[3] = getDomain(items[4]); // domain; 
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
			context.write(new Text(sb.toString()), new Text("1"));
		}
		
	}
	
	  
	
	public String getDomain(String curl){
		  URL url = null;
		  String q = "";
		  try {
		   url = new URL("http://"+curl);
		   q = url.getHost();
		  } catch (MalformedURLException e) {   
		 
		  }
		  url = null;
		  return q;
		 }
}

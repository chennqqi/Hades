package com.aotain.dw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserLastUrlCombiner extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		List<String> save = new ArrayList<String>();
		
		HashMap<String,Integer> referUrlList = new HashMap<String,Integer>();
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
			if(items.length < 6)
				continue;
			String referurl = items[5];
			if(!referurl.isEmpty() 
					&& referurl.substring(referurl.length()-1).equals("/"))
				referurl = referurl.substring(0,referurl.length() - 1);
			if(!referurl.isEmpty() && !referUrlList.containsKey(referurl))
			{
				referUrlList.put(referurl,0);
			}
			//String strGzip = ZipUtils.gzip(txt.toString());
			
			if(!referUrlList.containsKey(items[4]))
			{
				save.add(txt.toString());
			}
		}
		
		for(String s: save)
		{
			//s = ZipUtils.gunzip(s);
			String[] items = s.split("\\|",-1);
			if(!referUrlList.containsKey(items[4]))
			{
				//url不在refer中存在的记录存下来,当前url不能为空
				context.write(new Text(s), new Text(""));
			}
		}
	}
}

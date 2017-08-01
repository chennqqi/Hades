package com.aotain.ods;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IdentifierReducer   extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		Text v = new Text();
		Set<Object> rt1 = new HashSet<Object>();
		Set<Object> rt2 = new HashSet<Object>();
		Set<Object> rt3 = new HashSet<Object>();
		String date = context.getConfiguration().get("date");
		String[] vkey = key.toString().trim().split("\\|",-1);
		String flag="Phone";
		
		if(vkey[0].equals("1"))
		{
			flag="Mail";
		}
		else if(vkey[0].equals("2"))
		{
			flag="IMEI";
		}else if(vkey[0].equals("3"))
		{
			flag="MAC";
		}else if(vkey[0].equals("4"))
		{
			flag="IDFA";
		}
		
		
		for(Text t : values)
		{
			String[] splitvs = t.toString().trim().split("\\|",-1);
			if("1".equals(splitvs[0])){
				rt1.add(splitvs[1]);
			}else if("2".equals(splitvs[0])){
				rt2.add(splitvs[1]);
			}else if("3".equals(splitvs[0])){
				rt3.add(splitvs[1]);
			}
		}
		
		v.set(String.format("%s,%s,%s,%s,%s,",vkey[1],flag,vkey[2],rt1.size()+rt2.size()*2+rt3.size()*3,date));
		context.write(v, new Text(""));
	}

}

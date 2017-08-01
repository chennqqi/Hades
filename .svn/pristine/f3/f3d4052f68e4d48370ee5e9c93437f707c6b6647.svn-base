package com.aotain.ods;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class HTTPFilterReducer extends Reducer<Text,Text,Text,Text> {
	
	private MultipleOutputs<Text, Text> output;
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		
		//String strCurDate = context.getConfiguration().get("app.date");
		
		/*
		List<Text> save = new ArrayList<Text>();
		
		HashMap<String,String> referUrlList = new HashMap<String,String>();
		for(Text txt : values)
		{
			//ÐÐ×Ö¶ÎÖµ
			String[] items = txt.toString().split("\\|",-1);
			
			String referurl = items[5];
			if(!referurl.isEmpty() 
					&& referurl.substring(referurl.length()-1).equals("/"))
				referurl = referurl.substring(0,referurl.length() - 1);
			if(!referurl.isEmpty() && !referUrlList.containsKey(referurl))
			{
				referUrlList.put(referurl,"");
			}
			save.add(new Text(txt));
			
		}
		
		for(Text s : save)
		{
			String[] items = s.toString().split("\\|",-1);
			if(items[5].trim().isEmpty() || referUrlList.containsKey(items[4]))
			{
				
			}
		}*/
		String strKey = key.toString();
		String sDate = strKey.split("\\|",-1)[0];
		
		
		for(Text t : values)
		{
			if(sDate.equals("TODAY"))
			{
				context.write(new Text(t), new Text(""));
			}
			else
			{
				output.write(new Text(t), new Text(""), sDate);
			}
		}
		
	}
	
	
	@Override
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        output = new MultipleOutputs<Text, Text>(context);
    }
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		output.close();
    }
}

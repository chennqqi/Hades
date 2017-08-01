package com.aotain.ods.phone;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PhoneFileReducer   extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		int count = 0;
		Text v = new Text();
		String date = context.getConfiguration().get("date");
		String[] vkey = key.toString().trim().split("\\|",-1);
		for(Text t : values)
		{
			int  num=Integer.parseInt(t.toString());
			count+=num;
		}

		v.set(String.format("%s,%s,%s,%s,%s,",vkey[0],"Phone",vkey[1],count,date));
		context.write(v, new Text(""));
	}

}

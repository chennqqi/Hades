package com.aotain.ods;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MailFileReducer   extends Reducer<Text,Text,Text,Text>{
	private MultipleOutputs output;
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		Text v = new Text();
		Set<Object> ret = new HashSet<Object>();
		String date = context.getConfiguration().get("date");
		String[] vkey = key.toString().trim().split("\\|",-1);
		String flag="IDFA";
		
		
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
		}
		
		
		for(Text t : values)
		{
			ret.add(t);
		}
		
		v.set(String.format("%s,%s,%s,%s,%s,",vkey[1],flag,vkey[2],ret.size(),date));
		output.write(flag,v, new Text(""));
	}
	
	@Override
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        output = new MultipleOutputs(context);
    }
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		output.close();
    }
}

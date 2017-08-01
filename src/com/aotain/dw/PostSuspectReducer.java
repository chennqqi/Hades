package com.aotain.dw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PostSuspectReducer  extends Reducer<Text,Text,Text,Text>{
	private MultipleOutputs output;
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		
		ArrayList<String> lines = new ArrayList<String>();
		String sCardType = "";
		String oper = "";
		String cityname = "";
		String province = "";
		for(Text t : values)
		{
			String[] items = t.toString().split("\\|",-1);
			String strFileType = items[0];
			
			if(strFileType.equals("POST"))
			{//FILETYPE|USERNAME|DATATYPE|VALUE
				int datatype = Integer.parseInt(items[2]);
				if(datatype == 2)
				{//邮箱
					String v = String.format("%s|%s|", items[1],items[3]);
					output.write("MAIL",new Text(v), new Text(""));
					//context.write(new Text(v), new Text(""));
				}
				else if(datatype == 1)
				{//手机号
					lines.add(t.toString());
					
				}
			}
			else if(strFileType.equals("MOBILE"))
			{//PHONE|CARDTYPE
				sCardType = t.toString().split("\\|",-1)[1];
				if(sCardType.contains("电信"))
				{
					oper = "电信";
				}
				else if(sCardType.contains("联通"))
				{
					oper = "联通";
				}
				else if(sCardType.contains("移动"))
				{
					oper = "移动";
				}
				
				cityname = t.toString().split("\\|",-1)[2];
				province = t.toString().split("\\|",-1)[3];
			}
		}
		
		HashMap<String,String> mp = new HashMap();
		
		for(String s : lines)
		{
			String[] items = s.split("\\|",-1);
			
			String v = String.format("%s|%s|%s|%s|%s|%s|", 
					items[1],sCardType,items[3],oper,cityname,province);
			/*if(!mp.containsKey(v))
			{
				mp.put(v, "");
			}*/
			//context.write(new Text(v), new Text(""));
			output.write("MOBILE",new Text(v), new Text(""));
			
		}
		
		/*for(String v : mp.keySet())
		{
			output.write("MOBILE",new Text(v), new Text(""));
		}*/
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

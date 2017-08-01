package com.aotain.ods;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class DPIMobileReducer extends Reducer<Text,Text,Text,Text>{
private static HashMap<String,String> hmClass=null;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("app.domains");
		  hmClass  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException     
	{
		
			//String strDomain = context.getConfiguration().get("app.domains");
			//HashMap<String,String> hmClass = CommonFunction.getUrlClassConfig(strDomain);
			
			try
			{
			/**
			 * 	
				OUTPUT:
				   MDN                            STRING,
				   CDOMAIN                        STRING,
				   CAPPTAG                        STRING,
				   CURLTAG                        STRING,
				   CKEYTAG                        STRING,
				   ACCTIME                        STRING,
			 */
				
			for(Text value : values)
			{
				StringBuilder sbdResult = new StringBuilder();
				//sMDN|sDomainName|appflag|sUrlKeyword|sAccessTime
				
				String[] items = value.toString().split("\\|",-1);
				String MDN = items[0];
				String domain = items[1];
				String appflag = items[2];
				//String keyword = items[3]; 
				String accesstime = items[4];
				
				
				sbdResult.append(MDN + "|");//#0#
				sbdResult.append(domain + "|");//#1#
				sbdResult.append(appflag + "|");//#2#
				
				String urlflag = CommonFunction.getUrlClass(hmClass, domain);
				
				sbdResult.append(urlflag + "|");//#3#
				sbdResult.append(urlflag + "|");//#4#
				sbdResult.append(accesstime + "|");//#5#
				
				context.write(new Text(sbdResult.toString()), new Text(""));
				
			}
			
			//context.write(new Text(strDomain), new Text(""));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
}

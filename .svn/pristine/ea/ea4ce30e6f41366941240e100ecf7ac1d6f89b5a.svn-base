package com.aotain.dw;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.aotain.common.CommonFunction;
import com.aotain.common.ObjectSerializer;

public class DataCleaningTerminalReducer extends Reducer<Text,Text,Text,Text> {
	
	
	private static HashMap<String,String> arrList=null;
	
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		  String str = context.getConfiguration().get("app.devicelist");
		  arrList  = ( HashMap<String,String>) ObjectSerializer
		            .deserialize(str);
	}
	
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException        {
		
		//String sDeviceList = context.getConfiguration().get("app.devicelist");
		//String[] arrList = sDeviceList.split("\\|");
		//float fMaxRate = 0;
		String deviceName = "";
		String fullName = "";
		String DeviceType = "";
		/*for(Text value : values)
		{
			for(String mapKey : arrList.keySet())
			{
				String[] arrDev = mapKey.split(":",-1);
				float v = CommonFunction.Similar(value.toString(),arrDev[1]);
				if(v > fMaxRate && v >= 0.4)
				{
					fMaxRate = v;
					deviceName = arrList.get(mapKey);
					fullName = arrDev[1];
					DeviceType = arrDev[0];
				}
			}
			break;
		}*/
				
		key = new Text(key.toString()+ deviceName + "|" + fullName + "|" + DeviceType + "|");
		context.write(key, new Text("")); 
	} 
	
	
}

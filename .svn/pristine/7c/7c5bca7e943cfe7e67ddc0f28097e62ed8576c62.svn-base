package com.aotain.dw;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.aotain.common.CommonFunction;

public class PostSuspectMapper extends Mapper<LongWritable,Text,Text,Text>{

	public void map(LongWritable key,Text value,Context context) 
			throws IOException{
		String strFileType = "";
		String line = value.toString();
		String sUserName = "";
		
		if(line.contains("BroadbandAccount"))
		{
			strFileType = "POST";
			String[] items = value.toString().split("\\|",-1);
			int filedNum = 0;
			boolean flag = false;
			String dataType = "-1";
			//HashMap<Integer,String> valueMap = new HashMap<Integer,String>();
			for(String s : items)
			{
				if(flag)
				{//实际的数据
					
					flag = false;
					String v = "";
					if(s.contains("="))
					{
						v = s.split("=",-1)[1];
					}
					else
					{
						v = s;
					}
					
					if(dataType.equals("1"))
					{//手机
						
						v = CommonFunction.findByRegex(v, "^((13[0-9])|(15[^4,\\D])|(18[0,5-9])|(147))\\d{8}$", 0);
						if(v == null)
							return;
						
						String outputKey = v.substring(0,7);
						String outputValue = String.format("%s|%s|1|%s", strFileType,sUserName,v);
						try {
							context.write(new Text(outputKey), new Text(outputValue));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else if(dataType.equals("2"))
					{//邮箱
						//^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$
						//^([a-zA-Z0-9_\\-\\.]{1,15})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org)(\\]?)$
						v = CommonFunction.findByRegex(v, "^([a-zA-Z0-9_\\-\\.]{1,15})@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|(([a-zA-Z0-9\\-]+\\.)+))(com|cn|net|com.cn|org)(\\]?)$", 0);
						if(v == null)
							return;
						
						String outputKey = sUserName;
						String outputValue = String.format("%s|%s|2|%s", strFileType,sUserName,v);
						try {
							context.write(new Text(outputKey), new Text(outputValue));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					continue;
				}
				
				if(s.split("=",-1).length <= 1)
					return;
				String FieldName = s.split("=",-1)[0];
				if(FieldName.equals("BroadbandAccount"))
				{
					sUserName = s.split("=",-1)[1];
					if(sUserName.trim().isEmpty())
						return;
				}
				else if(FieldName.equals("FiledNum"))
				{
					//filedNum = Integer.parseInt(s.split("=",-1)[1]);
				}
				else if(FieldName.equals("datetype"))
				{//数据类型
					dataType = s.split("=",-1)[1];
					flag = true;
				}
			}
		}
		else
		{
			strFileType = "MOBILE";
			String[] items = line.split("\\|",-1);
			if(items.length != 6)
				return;
			//3443|1303471|471|呼和浩特|内蒙古联通130卡|内蒙古|呼和浩特
			String outputKey = items[1];
			String outputValue = String.format("%s|%s|%s|%s", 
					strFileType,items[4],items[3],items[5]);
			try {
				context.write(new Text(outputKey), new Text(outputValue));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		
	}
}

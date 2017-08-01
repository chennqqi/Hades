package com.aotain.dw;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PostFormatInfoMapper extends Mapper<LongWritable,Text,Text,Text>{
	 public void map(LongWritable key,Text value,Context context){

		 String sColumns = context.getConfiguration().get("app.columns");
		 String sDomains = context.getConfiguration().get("app.domains");
		 //System.out.println("List:"+sUrlList);
		 //获取到需要过滤的url
		 String[] arrColumn = sColumns.split(",");
		 String[] arrDomain = sDomains.split("\\|");
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 
		 if(items.length<3)
			 return;
		 
		 
		 
		
				 StringBuilder sb = new StringBuilder();
				 for(String column : arrColumn)
				 {
					 int counter = 0;
					 ArrayList<String> buffer = new ArrayList<String>();
					 for(String v : items)
					 {
						 if(v.split("=",-1).length<2)
							 continue;
						 if(buffer.contains(v))
							 continue;
						 
						 buffer.add(v);
						 
						 String name = v.split("=",-1)[0];
						 if(column.trim().toUpperCase().equals(name.trim().toUpperCase()))
						 {
							 if(counter>0)
								 sb.append("#");
							 sb.append(v.split("=",-1)[1].trim());
							 counter++;
						 }
					 }
				     sb.append("|");
				 }
				 
				 
				 
				 try {
					 String s=sb.toString();
					 String []info=s.split("\\|");
					
					 if(info.length!=1)
					 context.write(new Text(sb.toString()), new Text(""));
					} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		
	 }
}

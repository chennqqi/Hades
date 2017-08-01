package com.aotain.ods;

import java.io.IOException; 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PostMapper extends Mapper<LongWritable,Text,Text,Text>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         
		  	
		  String kvsplit = context.getConfiguration().get("kvsplit");//keyvalue分隔符
		  String fieldsplit = context.getConfiguration().get("fieldsplit");//列属性分隔符
		  String column = context.getConfiguration().get("column");
		  
		  String vkey="";
		  String[] items,tmps;

		  try
		  {
			//字段处理
			  String[] columns = column.split("#",-1);
			  items = value.toString().split("\\"+fieldsplit,columns.length+1);
			  //列与正则
			  for(int i = 0;i < items.length; i++)
			  {
				  if(i==0)
				  {
					  tmps=items[i].split(kvsplit,-1);
					  if(tmps.length<2)
						  return;
				  }
				  if(i==items.length-2)
				  {
					  tmps=items[i].split(kvsplit,-1);
					  SimpleDateFormat dt = new SimpleDateFormat("yyyyMMddHHmmSS");
					  Date dStartTime = new Date(Long.parseLong(tmps[1])*1000L);
					  vkey+=tmps[0]+kvsplit+dt.format(dStartTime)+",";
				  }
				  else
					  vkey+=items[i]+",";
			  }
	          context.write(new Text(vkey),new Text("")); 
		  }
     	 catch(Exception e)
     	 {;}
      }        
}

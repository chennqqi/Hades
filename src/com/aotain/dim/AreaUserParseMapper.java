package com.aotain.dim;

import java.io.IOException; 

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AreaUserParseMapper extends Mapper<LongWritable,Text,Text,Text>{
	  @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{                         
		  
		  String fieldsplit = "|";//列属性分隔符
		  String[] items;

		  try
		  {
			  //源文件据处理
			  items = value.toString().split("\\"+fieldsplit,-1);
			  if(items.length==4)
			  {
				  String ipcut = items[1];
				  int index1 = ipcut.indexOf("-");
				  int index2 = ipcut.lastIndexOf(".");
				  String iphead = ipcut.substring(0,index2+1);
				  for(int ipindex=Integer.parseInt(ipcut.substring(index2+1,index1));ipindex<=Integer.parseInt(ipcut.substring(index1+1));ipindex++)
				  {
					  context.write(new Text(items[0]+","+iphead+String.valueOf(ipindex)+","+items[2]+","+items[3]+","),new Text(""));
				  }
			  }
		 }
     	 catch(Exception e)
     	 {;}
      }        
}
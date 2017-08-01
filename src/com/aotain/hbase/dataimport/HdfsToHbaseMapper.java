package com.aotain.hbase.dataimport;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class HdfsToHbaseMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key,Text value,Context context){
		 
		 //用于hbase的rowkey
		 String sRowKey = context.getConfiguration().get("Hbase.rowkey");
		 
		 String sColumns = context.getConfiguration().get("Hbase.columns");
		 
		 //取到列名
		 String[] arrColumns = sColumns.split(",",-1);
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 
		 if(items.length < arrColumns.length)
			 return;
		
		 //System.out.println("ColumnName:"+sKeyIndex);
		 
		 String[] arrKeyIndex = sRowKey.split(",",-1);

		 //rowkey第一个字段用5位数字的随机数做平坦分布使用
		 int keyID = CommonFunction.generateRandom(10000, 89999);
		 String rowkey = String.valueOf(keyID);
		 
		 
         
		 
		 for(int i = 0;i < arrKeyIndex.length; i++)
		 {
			 if(arrKeyIndex[i].isEmpty())
				 continue;
			 if(arrColumns[Integer.parseInt(arrKeyIndex[i])].toUpperCase().equals("STARTTIME"))
		  	 {
		      	   SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");
		      	   Date dStartTime = new Date(Long.parseLong(items[Integer.parseInt(arrKeyIndex[i])])*1000L);
				   String sTime = df.format(dStartTime);
				   rowkey = rowkey + "_" + sTime;
		  	 }
			 else
			 {
				 rowkey = rowkey + "_" + items[Integer.parseInt(arrKeyIndex[i])];
			 }
		 }
		 
		 for(int i = 0;i < items.length; i++)
		 {
			 String hValue = arrColumns[i] + "|" +  items[i];
			 if(arrColumns[i].toUpperCase().equals("STARTTIME"))
      	   	 {//针对UDE数据的时间做格式转换
	        	   SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");
	        	   Date dStartTime = new Date(Long.parseLong(items[i])*1000L);
	        	   hValue = arrColumns[i] + "|" + df.format(dStartTime);
      	   	 }
			 
			 try {
				context.write(new Text(rowkey), new Text(hValue));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	 }	
}

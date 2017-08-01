package com.aotain.hbase.dataimport;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.aotain.common.CommonFunction;

public class HFileKVDataMapper extends 
	Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		
		 String kvsplit = context.getConfiguration().get("Hbase.kvsplit");//keyvalue分隔符
		 String fieldsplit = context.getConfiguration().get("Hbase.fieldsplit");//列属性分隔符
		 String sRowKey = context.getConfiguration().get("Hbase.rowkey");//rowkey标示
		 
		 //rowkey
		 String[] rowkeys = sRowKey.split(",");
		 HashMap<String,Integer> map_rowkeyflag = new HashMap();
		 for(int i = 0;i<rowkeys.length;i++)
		 {
			 map_rowkeyflag.put(rowkeys[i], 1);
		 }
		 
		 
		 //行字段值
		 String[] items = value.toString().split("\\" + fieldsplit,-1);
		 
		 //rowkey第一个字段用5位数字的随机数做平坦分布使用
		 int keyID = CommonFunction.generateRandom(10000, 99999);
		 //String rowkey= String.valueOf(keyID);
		 String rowkey = "";
		 
		 int cnt = 0;
		 for(int i = 0;i < items.length; i++)
		 {
			 String[] arr_kv = items[i].split(kvsplit,2);
			 if(map_rowkeyflag.get(arr_kv[0])==null)
				 continue;

			 rowkey += rowkey+arr_kv[1] +"#";
			 cnt++;
		 }
		 
		 if(cnt!=rowkeys.length)
			 return;
		 
		 ImmutableBytesWritable outputKey = new ImmutableBytesWritable(rowkey.getBytes());
		 
		 for(int i = 0;i < items.length; i++)
		 {
			 String[] arr_kv = items[i].split(kvsplit,2);
			 String hValue = arr_kv[1];
			 if(arr_kv[0].toUpperCase().contains("TIME"))
     	   	 {
				 //针对UDE数据的时间做格式转换
	        	   SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");
	        	   Date dStartTime = new Date(Long.parseLong(hValue)*1000L);
	        	   hValue = df.format(dStartTime);
     	   	 }
			 
			 KeyValue kv = new KeyValue(rowkey.getBytes(),"cf".getBytes(),
					 arr_kv[0].getBytes(),hValue.getBytes());
			 
			 try 
			 {
				 context.write(outputKey, kv);
			 } 
			 catch (IOException e) 
			 {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 } 
			 catch (InterruptedException e) 
			 {
				 // TODO Auto-generated catch block
				 e.printStackTrace();
			 }
		 }
    }
}

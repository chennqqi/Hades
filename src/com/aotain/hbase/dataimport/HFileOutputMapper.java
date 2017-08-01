package com.aotain.hbase.dataimport;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.aotain.common.CommonFunction;

public class HFileOutputMapper extends 
	Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
		
		
		 //用于hbase的rowkey columnindex|datalength
		 String sRowKey = context.getConfiguration().get("Hbase.rowkey");
		 
		 String sColumns = context.getConfiguration().get("Hbase.columns");
		 sColumns = sColumns.toUpperCase();
		 
		 //取到列名
		 String[] arrColumns = sColumns.split(",",-1);
		 
		 //行字段值
		 String[] items = value.toString().trim().split("\\|",-1);
		 
		 if(items.length < arrColumns.length)
			 return;
		
		 //System.out.println("ColumnName:"+sKeyIndex);
		 
		 String[] arrKeyIndex = sRowKey.split(",",-1);

		 //rowkey第一个字段用5位数字的随机数做平坦分布使用
		 int keyID = CommonFunction.generateRandom(1000, 9999);
		 //String rowkey = String.valueOf(keyID);
		 
		 String rowkey = "";
		 
		 Date timestamp = new Date();
		 String strDate = "";
		 for(int i = 0;i < arrKeyIndex.length; i++)
		 {
			 if(arrKeyIndex[i].isEmpty())
				 continue;
			 
			 String arrKey[] = arrKeyIndex[i].split("\\|");
			 int keyindex = Integer.parseInt(arrKey[0]);
			 
			 String Name = arrColumns[keyindex];
			 //String Name = rowkeys[0];
			 int datalength = 0;
			 if(arrKey.length>1)
				 datalength = Integer.parseInt(arrKey[1]);
			 
			  
			 if(Name.toUpperCase().equals("ACCESSTIME"))
		  	 {
				 String accesstime = CommonFunction.findByRegex(items[keyindex], "[0-9]*", 0);
				 if(accesstime == null || accesstime.trim().isEmpty())
					 return;
				 
				 SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			     Date dStartTime = new Date(Long.parseLong(items[keyindex])*1000L);
			     rowkey = rowkey  + items[keyindex] + "#";
			     timestamp = dStartTime;
			     strDate = df.format(dStartTime);
				 
		  	 }
			 /*else if(datalength!=0)
			 {//默认对username字段设置10个字符串，不足10位‘0’补全
				 String s = items[keyindex];
				 //075503647625@163.gd
				 s = StringUtils.rightPad(s, datalength, "0");
				 rowkey = rowkey  + s + "#";
			 }*/
			 else
			 {
				 rowkey = rowkey  + items[keyindex] + "#";
			 }
		 }
		 
		 rowkey = strDate + "#" + rowkey + String.valueOf(keyID);
		 
		 ImmutableBytesWritable outputKey = new ImmutableBytesWritable(rowkey.getBytes());
		 
		 String username = "";
		 String url = "";
		 
		 for(int i = 0;i < arrColumns.length; i++)
		 {
			 String hValue = items[i];
			 if(arrColumns[i].toUpperCase().equals("ACCESSTIME"))
     	   	 {//针对UDE数据的时间做格式转换
	        	   SimpleDateFormat df = new SimpleDateFormat("yyyyMMddhhmmss");
	        	   Date dStartTime = new Date(Long.parseLong(items[i])*1000L);
	        	   hValue = df.format(dStartTime);
     	   	 }
			 
			 /*if(arrColumns[i].toUpperCase().equals("USERNAME"))
			 {
				 username = hValue;
				 continue;
			 }
			 else if(arrColumns[i].toUpperCase().equals("URL"))
			 {
				 url = hValue;
				 continue;
			 }*/
			 
			 try {
				 
				 KeyValue kv = new KeyValue(rowkey.getBytes(),"cf".getBytes(),
						 arrColumns[i].getBytes(),(long)timestamp.getTime(),
						 hValue.getBytes());
				 
				 context.write(outputKey, kv);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
		 
		 if(!username.isEmpty())
		 {
			 KeyValue kv = new KeyValue(rowkey.getBytes(),"cf".getBytes(),
				 username.getBytes(),url.getBytes());
		 	 context.write(outputKey, kv);
		 }
    }

}

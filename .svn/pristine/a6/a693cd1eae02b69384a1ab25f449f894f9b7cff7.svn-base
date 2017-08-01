package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 从hdfs文件中获取数据形成map
 * @author Administrator
 *
 */
public class TestMapper extends Mapper<LongWritable,Text,Text,Text>{
	 public void map(LongWritable key,Text value,Context context){
		 String sColumns = context.getConfiguration().get("file.columns");
		 //System.out.println("ColumnName:" + sColumns);
		 //System.out.println("Value:" + value);
		 //取到列名
		 String[] arrColumns = sColumns.split(",",-1);
		 
		 //行字段值
		 String[] items = value.toString().split("\\|",-1);
		 
		 //用户做rowkey的索引
		 String sKeyIndex = context.getConfiguration().get("rowkey.index");
		 //System.out.println("ColumnName:"+sKeyIndex);
		 
		 String[] arrKeyIndex = sKeyIndex.split(",",-1);
		 //System.out.println("arrKeyIndex Num:" + arrKeyIndex.length); 
		 //拼接rowkey
		 String rowkey = "";
		 for(int i = 0;i < arrKeyIndex.length; i++)
		 {
			 if(arrKeyIndex[i].isEmpty())
				 continue;
			 rowkey = rowkey + "_" + items[Integer.parseInt(arrKeyIndex[i])];
		 }
		 
		 for(int i = 0;i < items.length; i++)
		 {
			 String hValue = arrColumns[i] + ":" +  items[i];
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

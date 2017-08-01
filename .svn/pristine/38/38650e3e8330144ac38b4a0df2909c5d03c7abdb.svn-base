package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class CountReducer extends  TableReducer <Text, IntWritable, Text>{
	 
	public void reduce(Text key,Iterable<IntWritable> values,
			   Context context) throws IOException, InterruptedException {
		  int count = 0;
		  String[] inputkeys = key.toString().split("\\|");
		  String rowkey = "";
		  for(String skey:inputkeys)
		  {
			  rowkey = rowkey + skey.split("=")[1] + "#";
		  }
		  rowkey = rowkey.substring(0,rowkey.length() - 1);
		  
		  Text bRowkey = new Text(rowkey);
		  Put put = new Put(rowkey.getBytes());
		  
		  for(String skey:inputkeys)
		  {
			  String k = skey.split("=")[0];
			  String v = skey.split("=")[1];
			  put.add("cf".getBytes(),k.getBytes(),v.getBytes("UTF8"));
		  }
		  
		  for (IntWritable val : values) {
			  count += val.get();
		  }
		  
		  put.add("cf".getBytes(),"count".getBytes(),Bytes.toBytes(String.valueOf(count)));

		  context.write(bRowkey,put);
	 }
}

package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class CountMapper extends TableMapper<Text, IntWritable>{
	 
	 @Override
	 public void map(ImmutableBytesWritable row, Result values,Context context)
			 throws IOException{
		 
		 //this.nameServer = context.getConfiguration().get("hbase.nameserver.address",null);
		 
	     String countkey = "";
	     //String countvalue = "";
	     // String confKey = context.getConfiguration().get("hbase.countkey");
	    // String datetime = context.getConfiguration().get("hbase.datetime");
	    // countkey = "";
	     //取到获取group by的 关键字段名称
	     //String[] arrkey = confKey.split(",");
	     
	     for(Cell cell : values.listCells())
	     {
	    	//byte[] value = values.getValue(Bytes.toBytes("cf"), Bytes.toBytes(key));
	    	//countkey = countkey  + Bytes.toString(CellUtil.cloneValue(cell)) + "#";
	    	 
	    	 countkey = countkey + Bytes.toString(CellUtil.cloneQualifier(cell)) 
	    			 + "=" + Bytes.toString(CellUtil.cloneValue(cell)).trim() + "|"; 
	     }
	     
	     //ImmutableBytesWritable outputKey = new ImmutableBytesWritable(countkey.getBytes());
	     
	     Text outputKey = new Text(countkey);
	     
	     IntWritable outputValue = new IntWritable(1);

	     
	     try {
				context.write(outputKey,outputValue);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	      //ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(countkey));
	      //ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(1));
	 }
	 
}

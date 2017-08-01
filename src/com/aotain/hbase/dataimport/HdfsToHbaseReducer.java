package com.aotain.hbase.dataimport;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HdfsToHbaseReducer 
	extends TableReducer<Text,Text,ImmutableBytesWritable>{
	
	public void reduce(Text key,Iterable<Text> value,Context context){
        
		String rowkey = key.toString();//rowkey
       
        Put putrow = new Put(rowkey.getBytes());
        /**
         * cf ап╢ь
         * qualifier: column key
         */
        
        
        for (Text txt : value) {
        	   String hcolumn = txt.toString().split("\\|",-1)[0];
        	   String hvalue = txt.toString().split("\\|",-1)[1];
        	   
               
               putrow.add("cf".getBytes(), hcolumn.getBytes(), hvalue.getBytes());
	    }
        
        try {
            
            context.write(new ImmutableBytesWritable(key.getBytes()), putrow);
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}

package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

/**
 * 将文件形成的map写到hbase表中
 * @author Administrator
 *
 */
public class TestReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
	public void reduce(Text key,Iterable<Text> value,Context context){
        
		String k = key.toString();//rowkey
        String v = value.iterator().next().toString(); //value的组成   column:value
        Put putrow = new Put(k.getBytes());
        /**
         * cf 列簇
         * qualifier: column key
         */
        
        String hcolumn = v.split(":",-1)[0];
        String hvalue = v.split(":",-1)[1];
        putrow.add("cf".getBytes(), hcolumn.getBytes(), hvalue.getBytes());
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

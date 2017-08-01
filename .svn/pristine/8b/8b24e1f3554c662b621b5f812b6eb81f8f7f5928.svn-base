package com.aotain.dw;


import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class UserMailReduceer extends TableReducer <ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> 
{
	 @Override
	 public void reduce(ImmutableBytesWritable key,Iterable<ImmutableBytesWritable> values,
			   Context context) throws IOException, InterruptedException 
	 {
        try
        {
        	Put putrow = new Put(key.get());
        	String mail = "";
        	for (ImmutableBytesWritable val : values) 
        	{
        		mail= Bytes.toString(val.get());
        	}
        	putrow.add("usermail".getBytes(), "account".getBytes(), key.get());
        	putrow.add("usermail".getBytes(), "mail".getBytes(), Bytes.toBytes(mail));

            context.write(new ImmutableBytesWritable(key.get()), putrow);
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
}

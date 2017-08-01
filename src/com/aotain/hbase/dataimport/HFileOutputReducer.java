package com.aotain.hbase.dataimport;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HFileOutputReducer extends Reducer<ImmutableBytesWritable, 
		KeyValue, ImmutableBytesWritable, KeyValue> {
	public void reduce(ImmutableBytesWritable key, Iterable<KeyValue> values,
	    Context context) throws IOException, InterruptedException {
	            //reducerÂß¼­²¿·Ö
		for(KeyValue kv:values)
		{
			context.write(key, kv);
		}
	}
}

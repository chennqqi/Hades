package com.aotain.dw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DomainStatReducer extends  Reducer <Text, IntWritable, Text, Text>{
	public void reduce(Text key,Iterable<IntWritable> values,
			   Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable val : values) {
			  count += val.get();
		  }
		
		String name = key.toString().split("_",-1)[0];
		String level = key.toString().split("_",-1)[1];
		String value = String.format("%s|%s|%d|", name,level,count);
		context.write(new Text(value),new Text(""));
	}
}

package com.aotain.project.other;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TmpHttpReducer extends Reducer<Text, Text, Text, NullWritable>{

		protected void reduce(Text rkey, java.lang.Iterable<Text> rvals, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
			context.write(rkey,null);
		};
}

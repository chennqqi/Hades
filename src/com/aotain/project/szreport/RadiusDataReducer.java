package com.aotain.project.szreport;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RadiusDataReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	protected void reduce(Text rkey, java.lang.Iterable<Text> rvalue, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,NullWritable>.Context context) throws java.io.IOException ,InterruptedException {
		try{
			context.write(new Text(rkey), null);
		}catch (Exception e) {
		}
		
	};
	
}

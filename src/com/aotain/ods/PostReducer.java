package com.aotain.ods;

import java.io.IOException; 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PostReducer extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
	         context.write(new Text(key.toString().trim()), new Text("")); 
     } 
}
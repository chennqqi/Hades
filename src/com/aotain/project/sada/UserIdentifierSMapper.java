package com.aotain.project.sada;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserIdentifierSMapper extends Mapper<LongWritable,Text,Text,Text>{
	 
	 
	public void map(LongWritable key,Text value,Context context) 
			throws IOException, InterruptedException{
		
		try
      {
			String[] items = value.toString().split(",", -1);
			context.write(new Text(items[0]+"|"+items[4]), new Text(items[1]+"|"+items[2]+"|"+items[3]));
		}
		catch (Exception e)  {;}
	}
	
	
    

    
}
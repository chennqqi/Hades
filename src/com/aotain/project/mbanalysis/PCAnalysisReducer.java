package com.aotain.project.mbanalysis;

import java.io.IOException; 
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PCAnalysisReducer extends Reducer<Text,Text,Text,Text> {
	 @Override
     public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException        {
		 
		  
		 
		 	String vkey = key.toString()+",";
		 	/*int max=0;*/
		 	try{
		 	   for(Text value:values){
		    	context.write(new Text(vkey), new Text(value+","));
		    }
		 		
		 
		 	}catch(Exception e){
		 		e.printStackTrace();
		 	}
		 	
     } 
}
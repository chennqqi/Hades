package com.aotain.project.mbanalysis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.aotain.common.CommonFunction;

public class MBAnalysisReducer extends Reducer<Text, Text, Text, Text>{
	protected void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws java.io.IOException ,InterruptedException {
		String key = rkey.toString()+",";
		//同一账号设备个数
		 Map<String,Set<String>>mobiletypecount=new HashMap<String, Set<String>>();
		
		for (Text text : rvalue) {
			if(text.toString().split(",").length==3){
			String type=text.toString().split(",")[2];
			String systemsys=text.toString().split(",")[0]+text.toString().split(",")[1];
			 if(mobiletypecount.get(type)!=null){
				 Set<String>set=mobiletypecount.get(type);
				 set.add(systemsys);
		 		    
		 		   
		 		 }else{
		 			 Set<String>set=new HashSet<String>();
		 			 set.add(systemsys);
		 			mobiletypecount.put(type, set);
		 		 }
			 }
		}
		 Iterator<Entry<String, Set<String>>>mobiletypecountresult=mobiletypecount.entrySet().iterator();
		  while(mobiletypecountresult.hasNext()){
		 		Entry<String, Set<String>> en=mobiletypecountresult.next();
		 		context.write(new Text(key+en.getValue().size()+","+en.getKey()+","), new Text(""));
		 	 }
		
	};

	
}

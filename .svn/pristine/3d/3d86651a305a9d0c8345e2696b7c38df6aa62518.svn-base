package com.aotain.project.szreport;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CountqqReducer extends Reducer<Text, Text, Text, Text>{
	MultipleOutputs<Text, Text> collector = null;
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
	collector.close();
	}


	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
	collector = new MultipleOutputs<Text, Text>(context);
	}
	@Override
	protected void reduce(Text rkey, Iterable<Text> rvalue, Context context) throws java.io.IOException ,InterruptedException {
		  Map<String,Integer>qqApperCount=new HashMap<String, Integer>();
		  String date = context.getConfiguration().get("date");
		  for(Text value:rvalue){
			  collector.write("qqnum",new Text(rkey.toString()+","+value), new Text(""),"qqnum/qqnum");
			  String qq=value.toString().split(",")[1];
				 if(qqApperCount.get(qq)!=null){
		 			int count=qqApperCount.get(qq);
		 		     count=count+1;
		 		    qqApperCount.put(qq, count);
		 		 }else{
		 			qqApperCount.put(qq, 1);
		 		 }
		  }
		  Iterator<Entry<String, Integer>>qqnum=qqApperCount.entrySet().iterator();
		  while(qqnum.hasNext()){
		 		Entry<String, Integer> en=qqnum.next();
		 		collector.write("qqstatistics",new Text(rkey.toString()+","+"QQAccount,"+en.getKey()+","+en.getValue()+","+date+","), new Text(""),"qqstatistics/qqstatistics");
		 	 }
		
	
	};

}

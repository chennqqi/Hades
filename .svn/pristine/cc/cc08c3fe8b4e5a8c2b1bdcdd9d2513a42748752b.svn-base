package com.aotain.project.dmp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AnalysisDataLabelReducer extends Reducer<Text, Text, Text, Text>{
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
		int count=0;
		String key=rkey.toString();
	
		  for(Text value:rvalue){
			  if(!key.contains("countDATA"))
			  collector.write(key,new Text(value), new Text(""),key+"/"+key);
			  else
				  count++;
			 
		  }
		 if(key.contains("countDATA")){
			 if(key.contains("keyword"))
			 count=count*3; 
			 String outpath=key.split("_")[1];
			 String outvalue=key.split("_")[2];
			 if(!key.contains("lablefreq"))
			    collector.write(outpath,new Text(outvalue+","+count+","), new Text(""),outpath+"/"+outpath);
			 else
				 collector.write(outpath,new Text(outvalue+"#@@@#"+count), new Text(""),outpath+"/"+outpath);
		 }
		
	
	};

}
